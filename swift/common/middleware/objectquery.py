# needed for etag validation using md5
import re
# for capturing zerovm stdout and stderr
import os
from subprocess import PIPE
import time
import traceback

# needed for spawning zerovm processes
#from subprocess import Popen, STDOUT, PIPE

# needed to limit the number of simultaneously running zerovms
from eventlet import GreenPool, sleep
from eventlet.green import select, subprocess
from eventlet.timeout import Timeout
import subprocess

# needed for parsing manifest files returned from zerovm
from string import split
from urllib import unquote
from hashlib import md5
from webob import Request, Response
from tempfile import mkstemp

# needed for error handling
from webob.exc import HTTPBadRequest, HTTPNotFound,\
    HTTPPreconditionFailed, HTTPRequestTimeout, HTTPUnprocessableEntity,\
    HTTPServiceUnavailable, HTTPLengthRequired, HTTPRequestEntityTooLarge,\
    HTTPInternalServerError

from swift.common.utils import normalize_timestamp,\
    fallocate, split_path, drop_buffer_cache,\
    get_logger
from swift.obj.server import DiskFile
from swift.common.constraints import check_mount, check_utf8
from swift.common.exceptions import DiskFileError, DiskFileNotExist

class ObjectQueryMiddleware(object):

    def __init__(self, app, conf, logger=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='obj-query')

        #self.devices = conf.get('devices', '/srv/node/')
        #self.mount_check = conf.get('mount_check', 'true').lower() in\
        #                   ('true', 't', '1', 'on', 'yes', 'y')
        #self.max_upload_time = int(conf.get('max_upload_time', 86400))
        #self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        #self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        #self.log_requests = conf.get('log_requests', 't')[:1].lower() == 't'

        self.zerovm_exename = set(i.strip() for i in conf.get('zerovm_exename', 'zerovm').split() if i.strip())
        self.zerovm_xparams = set(i.strip() for i in conf.get('zerovm_xparams', '').split() if i.strip())

        # maximum number of simultaneous running zerovms, others are queued
        self.zerovm_maxpool = int(conf.get('zerovm_maxpool', 2))

        # maximum length of queue of request awaiting zerovm executions
        self.zerovm_maxqueue = int(conf.get('zerovm_maxqueue', 3))

        # timeout for zerovm to finish execution
        self.zerovm_timeout = int(conf.get('zerovm_timeout', 5))

        # timeout for zerovm between TERM signal and KILL signal
        self.zerovm_kill_timeout = int(conf.get('zerovm_kill_timeout', 1))

        # maximum length of manifest line (both input and output)
        self.zerovm_maxmnfstline = int(conf.get('zerovm_maxmnfstline', 1024))

        # maximum number of lines in input and output manifest files
        self.zerovm_maxmnfstlines = int(conf.get('zerovm_maxmnfstlines', 128))

        # maximum input data file size
        self.zerovm_maxinput = int(conf.get('zerovm_maxinput', 256 * 1048576))

        self.zerovm_maxiops = int(conf.get('zerovm_maxiops', 1024 * 1048576))

        # maximum nexe size
        self.zerovm_maxnexe = int(conf.get('zerovm_maxnexe', 256 * 1048576))

        # maximum output data file size
        self.zerovm_maxoutput = int(conf.get('zerovm_maxoutput', 64 * 1048576)) # TODO this breaks some tests

        self.zerovm_maxchunksize = int(conf.get('zerovm_maxchunksize', 1024 * 1024))

        # max syscall number
        self.zerovm_maxsyscalls = int(conf.get('zerovm_maxsyscalls', 1024 * 1048576))

        # max nexe memory size
        self.zerovm_maxnexemem = int(conf.get('zerovm_maxnexemem', 4 * 1024 * 1048576))

        # hardcoded, we don't want to crush the server
        self.zerovm_stderr_size = 65536
        self.zerovm_stdout_size = 65536
        self.retcode_map = ('OK', 'Timed out', 'Killed', 'Output too long')

        self.fault_injection = conf.get('fault_injection', '') # for unit-tests.
        self.os_interface = os

        # green thread for zerovm execution
        self.zerovm_thrdpool = GreenPool(self.zerovm_maxpool)


    def zerovm_query(self, req):
        """Handle HTTP QUERY requests for the Swift Object Server."""

        # TODO: we need a way to execute without an input path
        zerovm_execute_only = False
        try:
            (device, partition, account, container, obj) =\
            split_path(unquote(req.path), 5, 5, True)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=req,
                content_type='text/plain')

        if self.zerovm_thrdpool.size != self.zerovm_maxpool:
            self.zerovm_thrdpool = GreenPool(self.zerovm_maxpool)
        if self.zerovm_thrdpool.free() <= 0\
        and self.zerovm_thrdpool.waiting() >= self.zerovm_maxqueue:
            return HTTPServiceUnavailable(body='Slot not available',
                request=req, content_type='text/plain')
        if self.app.mount_check and not check_mount(self.app.devices, device):
            return Response(status='507 %s is not mounted' % device)
        if 'content-length' not in req.headers\
        and req.headers.get('transfer-encoding') != 'chunked':
            return HTTPLengthRequired(request=req)
        if 'content-length' in req.headers\
        and int(req.headers['content-length']) > self.zerovm_maxnexe:
            return HTTPRequestEntityTooLarge(body='Your request is too large.'
                , request=req, content_type='text/plain')
        if 'Content-Type' not in req.headers:
            return HTTPBadRequest(request=req, content_type='text/plain'
                , body='No content type')
        if req.headers['Content-Type'] != 'application/octet-stream':
            return HTTPBadRequest(request=req,
                body='Invalid Content-Type',
                content_type='text/plain')

        def get_multi_header(hdr_count,hdr_prefix):
            result = []
            if hdr_count not in req.headers:
                return result
            for c in xrange(0,int(req.headers[hdr_count])):
                channel = hdr_prefix+str(c)
                if channel not in req.headers:
                    return HTTPBadRequest(request=req,
                        body='Missing header %s' % channel,
                        content_type='text/plain')
                result[c] = req.headers[channel]
            return result

        channel_list = ''
        channels = get_multi_header('x-nexe-channels','x-nexe-channel-')
        for channel in channels:
            channel_list += 'Channel=%s\n' % channel

        nexe_std_list = []
        if 'x-nexe-stdlist' in req.headers:
            nexe_std_list = re.split('\s*,\s*', req.headers['x-nexe-stdlist'])
        if not nexe_std_list:
            nexe_std_list = ['stdout']

        file = DiskFile(
            self.app.devices,
            device,
            partition,
            account,
            container,
            obj,
            self.logger,
            disk_chunk_size=self.app.disk_chunk_size,
        )
        try:
            input_file_size = file.get_data_file_size()
        except (DiskFileError, DiskFileNotExist):
            return HTTPNotFound(request=req)
        if input_file_size > self.zerovm_maxinput:
            return HTTPRequestEntityTooLarge(body='Data Object is too large'
                , request=req, content_type='text/plain')
        with file.mkstemp() as (zerovm_nexe_fd, zerovm_nexe_fn):
            if 'content-length' in req.headers:
                fallocate(zerovm_nexe_fd,
                    int(req.headers['content-length']))
            reader = req.environ['wsgi.input'].read
            upload_size = 0
            etag = md5()
            upload_expiration = time.time() + self.app.max_upload_time
            for chunk in iter(lambda: reader(self.app.network_chunk_size),
                ''):
                upload_size += len(chunk)
                if time.time() > upload_expiration:
                    return HTTPRequestTimeout(request=req)
                etag.update(chunk)
                while chunk:
                    written = os.write(zerovm_nexe_fd, chunk)
                    chunk = chunk[written:]
            if 'content-length' in req.headers\
            and int(req.headers['content-length']) != upload_size:
                return Response(status='499 Client Disconnect')
            etag = etag.hexdigest()
            if 'etag' in req.headers and req.headers['etag'].lower()\
            != etag:
                return HTTPUnprocessableEntity(request=req)

            def file_iter(fd_list, fn_list):
                """Returns an iterator over the data file."""

                try:
                    chunk_size = file.disk_chunk_size
                    for fd in fd_list:
                        read = 0
                        dropped_cache = 0
                        os.lseek(fd, 0, os.SEEK_SET)
                        while True:
                            chunk = os.read(fd, chunk_size)
                            if chunk:
                                read += len(chunk)
                                if read - dropped_cache > self.zerovm_maxchunksize:
                                    drop_buffer_cache(fd, dropped_cache,
                                        read - dropped_cache)
                                    dropped_cache = read
                                yield chunk
                            else:
                                drop_buffer_cache(fd, dropped_cache, read
                                - dropped_cache)
                                break
                finally:
                    try:
                        for fd in fd_list:
                            self.os_interface.close(fd)
                    except OSError:
                        pass
                    try:
                        for fn in fn_list:
                            self.os_interface.unlink(fn)
                    except OSError:
                        pass


            try:
                fd_list = []
                fn_list = []
                for stream in nexe_std_list:
                    if 'stdout' in stream:
                        (nexe_output_fd, nexe_output_fn) = mkstemp(dir=file.tmpdir)
                        fallocate(nexe_output_fd, self.zerovm_maxoutput)
                        fd_list.append(nexe_output_fd)
                        fn_list.append(nexe_output_fn)
                    elif 'stderr' in stream:
                        (nexe_error_fd, nexe_error_fn) = mkstemp(dir=file.tmpdir)
                        fallocate(nexe_error_fd, self.zerovm_maxoutput)
                        fd_list.append(nexe_error_fd)
                        fn_list.append(nexe_error_fn)
                # outputiter is responsible to delete temp file
                outputiter = file_iter(fd_list,fn_list)
            except:
                try:
                    if nexe_output_fd:
                        self.os_interface.close(nexe_output_fd)
                    if nexe_error_fd:
                        self.os_interface.close(nexe_error_fd)
                except OSError:
                    pass
                try:
                    if nexe_output_fn:
                        self.os_interface.unlink(nexe_output_fn)
                    if nexe_error_fn:
                        self.os_interface.unlink(nexe_error_fn)
                except OSError:
                    pass
                raise

            with file.mkstemp() as (zerovm_inputmnfst_fd,
                                    zerovm_inputmnfst_fn):
                nexe_input_fn = file.data_file
                zerovm_inputmnfst = (
                    'Version=13072012\n'
                    'Nexe=%s\n'
                    'NexeMax=%s\n'
                    'SyscallsMax=%s\n'
                    'NexeEtag=%s\n'
                    'Timeout=%s\n'
                    'MemMax=%s\n'
                    % (
                        zerovm_nexe_fn,
                        self.zerovm_maxnexe,
                        self.zerovm_maxsyscalls,
                        etag,
                        self.zerovm_timeout * 1000,
                        self.zerovm_maxnexemem
                        ))

#                    for (key, value) in file.metadata.iteritems():
#                        if key.lower().startswith('x-object-meta-'):
#                            zerovm_inputmnfst_part2 +=\
#                            '#x-data-attr       =' + key[14:] + ':'\
#                            + value + '\n'
#                    for header in req.headers:
#                        if header.lower().startswith('x-object-meta-'):
#                            zerovm_inputmnfst_part2 +=\
#                            '#x-nexe-attr       =' + header[14:]\
#                            + ':' + req.headers[header] + '\n'

                zerovm_inputmnfst += 'Channel=%s,/dev/stdin,4,%s,%s,0,0\n'\
                % ('/dev/null' if zerovm_execute_only else nexe_input_fn,
                   self.zerovm_maxiops, self.zerovm_maxinput)

                zerovm_inputmnfst += 'Channel=%s,/dev/stdout,5,0,0,%s,%s\n'\
                % (nexe_output_fn if 'stdout' in nexe_std_list else '/dev/null',
                   self.zerovm_maxiops, self.zerovm_maxoutput)

                zerovm_inputmnfst += 'Channel=%s,/dev/stderr,6,0,0,%s,%s\n'\
                % (nexe_error_fn if 'stderr' in nexe_std_list else '/dev/null',
                   self.zerovm_maxiops, self.zerovm_maxoutput)

                for channel in channels:
                    zerovm_inputmnfst += 'Channel=%s\n' % channel

                if 'x-nexe-env' in req.headers:
                    zerovm_inputmnfst += 'Environment=%s\n' \
                    % req.headers['x-nexe-env']

                if 'x-nexe-command-line' in req.headers:
                    zerovm_inputmnfst += 'CommandLine=%s\n' \
                    % req.headers['x-nexe-command-line']

                while zerovm_inputmnfst:
                    written = os.write(zerovm_inputmnfst_fd,
                        zerovm_inputmnfst)
                    zerovm_inputmnfst = zerovm_inputmnfst[written:]

                def ex_zerovm():
                    cmdline = []
                    cmdline += self.zerovm_exename
                    if len(self.zerovm_xparams) > 0:
                        cmdline += self.zerovm_xparams
                    cmdline += ['-M%s' % zerovm_inputmnfst_fn]
                    proc = subprocess.Popen(cmdline, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)

                    stdout_data = ''
                    stderr_data = ''
                    readable = [proc.stdout, proc.stderr]
                    start = time.time()
                    def get_output(stdout_data, stderr_data):
                        (data1, data2) = proc.communicate()
                        stdout_data += data1
                        stderr_data += data2
                        return stdout_data, stderr_data
                    while time.time() - start < self.zerovm_timeout:
                        rlist, wlist, xlist = \
                        select.select(readable, [], [], start - time.time() + self.zerovm_timeout)
                        if not rlist:
                            continue
                        for stream in rlist:
                            data = os.read(stream.fileno(), 4096)
                            if not data:
                                readable.remove(stream)
                                continue
                            if stream == proc.stdout:
                                stdout_data += data
                            elif stream == proc.stderr:
                                stderr_data += data
                            if len(stdout_data) > self.zerovm_stdout_size \
                            or len(stderr_data) > self.zerovm_stderr_size:
                                proc.kill()
                                return 3, stdout_data, stderr_data
                        if proc.poll() is not None:
                            stdout_data, stderr_data = get_output(stdout_data, stderr_data)
                            return 0, stdout_data, stderr_data
                        sleep(0.1)
                    if proc.poll() is None:
                        proc.terminate()
                        start = time.time()
                        while time.time() - start\
                        < self.zerovm_kill_timeout:
                            if proc.poll() is not None:
                                stdout_data, stderr_data = get_output(stdout_data, stderr_data)
                                return 1, stdout_data, stderr_data
                            sleep(0.1)
                        proc.kill()
                        stdout_data, stderr_data = get_output(stdout_data, stderr_data)
                        return 2, stdout_data, stderr_data

                thrd = self.zerovm_thrdpool.spawn(ex_zerovm)
                (zerovm_retcode, zerovm_stdout, zerovm_stderr) = thrd.wait()
                if zerovm_retcode:
                    raise Exception('ERROR OBJ.QUERY retcode=%s, '\
                                    ' zerovm_stdout=%s'\
                                    ' zerovm_stderr=%s'\
                                    % (self.retcode_map[zerovm_retcode], zerovm_stdout, zerovm_stderr))
                report = zerovm_stdout.splitlines()
                nexe_retcode = int(report[0])
                nexe_etag = report[1]
                nexe_status = '\n'.join(report[2:])

                #os.lseek(zerovm_outputmnfst_fd, 0, os.SEEK_SET)

                #if os.stat(zerovm_inputmnfst_fn).st_size >\
                #   (self.zerovm_maxmnfstline * self.zerovm_maxmnfstlines):
                #    raise Exception("Input manifest must be smaller"
                #                    " than %d." % (self.zerovm_maxmnfstline
                #                                   * self.zerovm_maxmnfstlines))
                #zerovm_outputmnfst =\
                #split(os.read(zerovm_outputmnfst_fd,
                #    self.zerovm_maxmnfstline
                #    * self.zerovm_maxmnfstlines), '\n',
                #    self.zerovm_maxmnfstlines)

#                    def retrieve_mnfst_field(
#                            i,
#                            n,
#                            optional=False,
#                            isint=True,
#                            rgx=None,
#                            ):
#                        if not (zerovm_outputmnfst[i])[0:19] == n:
#                            if optional:
#                                return None
#                            else:
#                                raise Exception('omnfst: expecting %s got %s,'\
#                                                ' retcode=%s, status=>%s' % (n,
#                                                                             (zerovm_outputmnfst[i])[0:19],
#                                                                             exor_retcode, exor_status))
#                        if not zerovm_outputmnfst[i][19] == '=':
#                            raise Exception('omnfst: expecting = at %s got %s'
#                                            ' retcode=%s, status=>%s' % (n,
#                                                                         zerovm_outputmnfst[i][19],
#                                                                         exor_retcode, exor_status))
#                        v = (zerovm_outputmnfst[i])[20:]
#                        if isint:
#                            v = int(v)
#                        if rgx:
#                            if not re.match(rgx, v):
#                                raise Exception('mnfst: %s does not match %s'
#                                % (n, rgx))
#                        return v

#                    zerovm_retcode = retrieve_mnfst_field(0,
#                        'zerovm_retcode     ')
#                    zerovm_status = retrieve_mnfst_field(1,
#                        'zerovm_status      ', isint=False)
#                    if zerovm_retcode:
#                        raise Exception('ERROR OBJ.QUERY zerovm_retcode=%s,'\
#                                        ' zerovm_status=%s' % (zerovm_retcode,
#                                                               zerovm_status))
#                    etag = retrieve_mnfst_field(2, 'etag               '
#                        , isint=False, rgx=r"([a-fA-F\d]{32})")
#                    nexe_retcode = retrieve_mnfst_field(3,
#                        '#retcode           ', optional=True)
#                    nexe_status = retrieve_mnfst_field(4,
#                        '#status            ', optional=True,
#                        isint=False)
#                    nexe_content_type = retrieve_mnfst_field(5,
#                        '#content-type      ', optional=True,
#                        isint=False)
#                    i = 6
                response = Response(app_iter=outputiter,
                    request=req, conditional_response=True)
#                while True:
#                    nexe_meta = retrieve_mnfst_field(i,
#                        '#x-data-attr       ', optional=True,
#                        isint=False)
#                    if nexe_meta:
#                        i += 1
#                        (name, val) = split(nexe_meta, ':')
#                        response.headers['X-Object-Meta-' + name] =\
#                        val
#                    else:
#                        break
                response.headers['x-nexe-retcode'] = nexe_retcode
                response.headers['x-nexe-status'] = nexe_status
                response.headers['x-nexe-etag'] = nexe_etag
                response.headers['X-Timestamp'] =\
                normalize_timestamp(time.time())
                content_length = 0
                for fn in fn_list:
                    content_length += os.path.getsize(fn)
                response.content_length = content_length
                response.headers['x-nexe-stdsize'] = ' '.join([str(os.path.getsize(fn)) for fn in fn_list])
                if 'x-nexe-content-type' in req.headers:
                    response.headers['Content-Type'] = req.headers['x-nexe-content-type']
                return req.get_response(response)

    def __call__(self, env, start_response):
        """WSGI Application entry point for the Swift Object Server."""
        start_time = time.time()
        req = Request(env)
        self.logger.txn_id = req.headers.get('x-trans-id', None)
        if not check_utf8(req.path_info):
            res = HTTPPreconditionFailed(body='Invalid UTF8')
        else:
            try:
                if req.method == 'POST' and 'x-zerovm-execute' in req.headers:
                    res = self.zerovm_query(req)
                else:
                    return self.app(env, start_response)
            except (Exception, Timeout):
                self.logger.exception(_('ERROR __call__ error with %(method)s'
                                        ' %(path)s '), {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = time.time() - start_time
        if self.app.log_requests:
            log_line = '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %.4f' % (
                req.remote_addr,
                time.strftime('%d/%b/%Y:%H:%M:%S +0000',
                    time.gmtime()),
                req.method, req.path, res.status.split()[0],
                res.content_length or '-', req.referer or '-',
                req.headers.get('x-trans-id', '-'),
                req.user_agent or '-',
                trans_time)

            self.logger.info(log_line)

        return res(env, start_response)


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def obj_query_filter(app):
        return ObjectQueryMiddleware(app, conf)
    return obj_query_filter