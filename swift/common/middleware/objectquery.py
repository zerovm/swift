# needed for etag validation using md5
from contextlib import contextmanager
import re
# for capturing zerovm stdout and stderr
import os
import time
import traceback

# needed to limit the number of simultaneously running zerovms
from eventlet import GreenPool, sleep
from eventlet.green import select, subprocess
from eventlet.timeout import Timeout

# needed for parsing manifest files returned from zerovm
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
    get_logger, mkdirs
from swift.obj.server import DiskFile, write_metadata
from swift.common.constraints import check_mount, check_utf8
from swift.common.exceptions import DiskFileError, DiskFileNotExist

class TmpDir(object):
    def __init__(self, path, device, disk_chunk_size=65536, os_interface=os):
        self.os_interface = os_interface
        self.tmpdir = self.os_interface.path.join(path, device, 'tmp')
        self.disk_chunk_size = disk_chunk_size

    @contextmanager
    def mkstemp(self):
        """Contextmanager to make a temporary file."""
        if not self.os_interface.path.exists(self.tmpdir):
            mkdirs(self.tmpdir)
        fd, tmppath = mkstemp(dir=self.tmpdir)
        try:
            yield fd, tmppath
        finally:
            try:
                self.os_interface.close(fd)
            except OSError:
                pass
            try:
                self.os_interface.unlink(tmppath)
            except OSError:
                pass

class ObjectQueryMiddleware(object):

    def __init__(self, app, conf, logger=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='obj-query')

        self.zerovm_manifest_ver = conf.get('zerovm_manifest_ver','09082012')
        self.zerovm_exename = set(i.strip() for i in conf.get('zerovm_exename', 'zerovm').split() if i.strip())
        #self.zerovm_xparams = set(i.strip() for i in conf.get('zerovm_xparams', '').split() if i.strip())

        # maximum number of simultaneous running zerovms, others are queued
        self.zerovm_maxpool = int(conf.get('zerovm_maxpool', 10))

        # maximum length of queue of request awaiting zerovm executions
        self.zerovm_maxqueue = int(conf.get('zerovm_maxqueue', 3))

        # timeout for zerovm to finish execution
        self.zerovm_timeout = int(conf.get('zerovm_timeout', 5))

        # timeout for zerovm between TERM signal and KILL signal
        self.zerovm_kill_timeout = int(conf.get('zerovm_kill_timeout', 1))

        # maximum length of manifest line (both input and output)
        #self.zerovm_maxmnfstline = int(conf.get('zerovm_maxmnfstline', 1024))

        # maximum number of lines in input and output manifest files
        #self.zerovm_maxmnfstlines = int(conf.get('zerovm_maxmnfstlines', 128))

        # maximum input data file size
        self.zerovm_maxinput = int(conf.get('zerovm_maxinput', 256 * 1048576))

        self.zerovm_maxiops = int(conf.get('zerovm_maxiops', 1024 * 1048576))

        # maximum nexe size
        self.zerovm_maxnexe = int(conf.get('zerovm_maxnexe', 256 * 1048576))

        # maximum output data file size
        self.zerovm_maxoutput = int(conf.get('zerovm_maxoutput', 64 * 1048576))

        self.zerovm_maxchunksize = int(conf.get('zerovm_maxchunksize', 1024 * 1024))

        # max syscall number
        self.zerovm_maxsyscalls = int(conf.get('zerovm_maxsyscalls', 1024 * 1048576))

        # max nexe memory size
        self.zerovm_maxnexemem = int(conf.get('zerovm_maxnexemem', 4 * 1024 * 1048576))

        # hardcoded, we don't want to crush the server
        self.zerovm_stderr_size = 65536
        self.zerovm_stdout_size = 65536
        self.retcode_map = ('OK', 'Error', 'Timed out', 'Killed', 'Output too long')

        self.fault_injection = conf.get('fault_injection', ' ') # for unit-tests.
        self.os_interface = os

        # green thread for zerovm execution
        self.zerovm_thrdpool = GreenPool(self.zerovm_maxpool)

    def execute_zerovm(self, zerovm_inputmnfst_fn):
        cmdline = []
        cmdline += self.zerovm_exename
        #if len(self.zerovm_xparams) > 0:
        #    cmdline += self.zerovm_xparams
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
            rlist, wlist, xlist =\
            select.select(readable, [], [], start - time.time() + self.zerovm_timeout)
            if not rlist:
                continue
            for stream in rlist:
                data = self.os_interface.read(stream.fileno(), 4096)
                if not data:
                    readable.remove(stream)
                    continue
                if stream == proc.stdout:
                    stdout_data += data
                elif stream == proc.stderr:
                    stderr_data += data
                if len(stdout_data) > self.zerovm_stdout_size\
                or len(stderr_data) > self.zerovm_stderr_size:
                    proc.kill()
                    return 4, stdout_data, stderr_data
            if proc.poll() is not None:
                stdout_data, stderr_data = get_output(stdout_data, stderr_data)
                ret = 0
                if proc.returncode:
                    ret = 1
                return ret, stdout_data, stderr_data
            sleep(0.1)
        if proc.poll() is None:
            proc.terminate()
            start = time.time()
            while time.time() - start\
            < self.zerovm_kill_timeout:
                if proc.poll() is not None:
                    stdout_data, stderr_data = get_output(stdout_data, stderr_data)
                    return 2, stdout_data, stderr_data
                sleep(0.1)
            proc.kill()
            stdout_data, stderr_data = get_output(stdout_data, stderr_data)
            return 3, stdout_data, stderr_data

    def zerovm_query(self, req):
        """Handle HTTP QUERY requests for the Swift Object Server."""

        nexe_headers = {
            'x-nexe-retcode': 0,
            'x-nexe-status': 'Zerovm did not run',
            'x-nexe-etag': '',
            'x-nexe-validation': 0,
            'x-nexe-cdr-line': '0 0 0 0 0 0 0 0 0 0 0 0'
        }
        if 'x-validator-exec' in req.headers:
            validator = str(req.headers.get('x-validator-exec', ''))
            if 'skip' in validator:
                self.zerovm_exename.append('-s')
            #if 'fuzzy' in validator:
            #    self.zerovm_exename.append('-z')
        if 'x-node-name' in req.headers:
            nexe_name = re.split('\s*,\s*', req.headers['x-node-name'])
            nexe_headers['x-nexe-name'] = nexe_name[0]
        zerovm_execute_only = False
        try:
            (device, partition, account) = \
                split_path(unquote(req.path), 3, 3)
            zerovm_execute_only = True
        except ValueError:
            pass
        if not zerovm_execute_only:
            try:
                (device, partition, account, container, obj) =\
                split_path(unquote(req.path), 5, 5, True)
            except ValueError, err:
                return HTTPBadRequest(body=str(err), request=req,
                    content_type='text/plain')

        #if self.zerovm_thrdpool.size != self.zerovm_maxpool:
        #    self.zerovm_thrdpool = GreenPool(self.zerovm_maxpool)
        if self.zerovm_thrdpool.free() <= 0\
        and self.zerovm_thrdpool.waiting() >= self.zerovm_maxqueue:
            return HTTPServiceUnavailable(body='Slot not available',
                request=req, content_type='text/plain', headers=nexe_headers)
        if self.app.mount_check and not check_mount(self.app.devices, device):
            return Response(status='507 %s is not mounted' % device, headers=nexe_headers)
        if 'content-length' not in req.headers\
        and req.headers.get('transfer-encoding') != 'chunked':
            return HTTPLengthRequired(request=req, headers=nexe_headers)
        if 'content-length' in req.headers\
        and int(req.headers['content-length']) > self.zerovm_maxnexe:
            return HTTPRequestEntityTooLarge(body='Your request is too large.'
                , request=req, content_type='text/plain', headers=nexe_headers)
        if 'Content-Type' not in req.headers:
            return HTTPBadRequest(request=req, content_type='text/plain'
                , body='No content type', headers=nexe_headers)
        if req.headers['Content-Type'] != 'application/octet-stream':
            return HTTPBadRequest(request=req,
                body='Invalid Content-Type',
                content_type='text/plain', headers=nexe_headers)

        channels = []
        if 'x-nexe-channels' in req.headers:
            for c in xrange(0,int(req.headers['x-nexe-channels'])):
                channel = 'x-nexe-channel-'+str(c)
                if not channel in req.headers:
                    return HTTPBadRequest(request=req,
                        body='Missing header %s' % channel,
                        content_type='text/plain',
                        headers=nexe_headers)
                channels.append(req.headers[channel])
        channel_list = ''
        for channel in channels:
            channel_list += 'Channel=%s\n' % channel

        nexe_std_list = []
        if 'x-nexe-stdlist' in req.headers:
            nexe_std_list = re.split('\s*,\s*', req.headers['x-nexe-stdlist'])
        if not nexe_std_list:
            nexe_std_list = ['stdout']

        if zerovm_execute_only:
            file = TmpDir(
                self.app.devices,
                device,
                disk_chunk_size=self.app.disk_chunk_size,
                os_interface=self.os_interface
            )
        else:
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
                return HTTPNotFound(request=req, headers=nexe_headers)
            if input_file_size > self.zerovm_maxinput:
                return HTTPRequestEntityTooLarge(body='Data Object is too large'
                    , request=req, content_type='text/plain', headers=nexe_headers)

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
                    return HTTPRequestTimeout(request=req, headers=nexe_headers)
                etag.update(chunk)
                while chunk:
                    written = self.os_interface.write(zerovm_nexe_fd, chunk)
                    chunk = chunk[written:]
                    sleep()
            if 'content-length' in req.headers\
            and int(req.headers['content-length']) != upload_size:
                return Response(status='499 Client Disconnect', headers=nexe_headers)
            etag = etag.hexdigest()
            if 'etag' in req.headers and req.headers['etag'].lower()\
            != etag:
                return HTTPUnprocessableEntity(request=req, headers=nexe_headers)

            def file_iter(fd_list, fn_list):
                """Returns an iterator over the data file."""

                try:
                    chunk_size = file.disk_chunk_size
                    for fd in fd_list:
                        read = 0
                        dropped_cache = 0
                        self.os_interface.lseek(fd, 0, self.os_interface.SEEK_SET)
                        while True:
                            chunk = self.os_interface.read(fd, chunk_size)
                            if chunk:
                                read += len(chunk)
                                if read - dropped_cache > self.zerovm_maxchunksize:
                                    drop_buffer_cache(fd, dropped_cache,
                                        read - dropped_cache)
                                    dropped_cache = read
                                yield chunk
                                sleep()
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

            nexe_output_fd = nexe_output_fn = None
            nexe_error_fd = nexe_error_fn = None
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
                zerovm_inputmnfst = (
                    'Version=%s\n'
                    'Nexe=%s\n'
                    'NexeMax=%s\n'
                    'SyscallsMax=%s\n'
                    'NexeEtag=%s\n'
                    'Timeout=%s\n'
                    'MemMax=%s\n'
                    % (
                        self.zerovm_manifest_ver,
                        zerovm_nexe_fn,
                        self.zerovm_maxnexe,
                        self.zerovm_maxsyscalls,
                        etag,
                        self.zerovm_timeout,
                        self.zerovm_maxnexemem
                        ))

                zerovm_inputmnfst += 'Channel=%s,/dev/stdin,0,%s,%s,0,0\n'\
                % ('/dev/null' if zerovm_execute_only else file.data_file,
                   self.zerovm_maxiops, self.zerovm_maxinput)

                zerovm_inputmnfst += 'Channel=%s,/dev/stdout,0,0,0,%s,%s\n'\
                % (nexe_output_fn if 'stdout' in nexe_std_list else '/dev/null',
                   self.zerovm_maxiops, self.zerovm_maxoutput)

                zerovm_inputmnfst += 'Channel=%s,/dev/stderr,0,0,0,%s,%s\n'\
                % (nexe_error_fn if 'stderr' in nexe_std_list else '/dev/null',
                   self.zerovm_maxiops, self.zerovm_maxoutput)

                for channel in channels:
                    zerovm_inputmnfst += 'Channel=%s\n' % channel

                if 'x-nexe-env' in req.headers:
                    zerovm_inputmnfst += 'Environment=%s\n' \
                    % req.headers['x-nexe-env']

                if 'x-nexe-args' in req.headers:
                    zerovm_inputmnfst += 'CommandLine=%s\n' \
                    % req.headers['x-nexe-args']

                nexe_name = None
                if 'x-node-name' in req.headers:
                    zerovm_inputmnfst += 'NodeName=%s\n'\
                    % req.headers['x-node-name']
                    nexe_name = re.split('\s*,\s*', req.headers['x-node-name'])

                if 'x-name-service' in req.headers:
                    zerovm_inputmnfst += 'NameServer=%s\n'\
                    % req.headers['x-name-service']

                while zerovm_inputmnfst:
                    written = self.os_interface.write(zerovm_inputmnfst_fd,
                        zerovm_inputmnfst)
                    zerovm_inputmnfst = zerovm_inputmnfst[written:]

                thrd = self.zerovm_thrdpool.spawn(self.execute_zerovm, zerovm_inputmnfst_fn)
                (zerovm_retcode, zerovm_stdout, zerovm_stderr) = thrd.wait()
                if zerovm_retcode:
                    err = 'ERROR OBJ.QUERY retcode=%s, '\
                          ' zerovm_stdout=%s'\
                            % (self.retcode_map[zerovm_retcode],
                               zerovm_stdout)
                    self.logger.exception(err)
                    resp = Response(body=err,status='503 Internal Error')
                    nexe_headers['x-nexe-status'] = 'ZeroVM runtime error'
                    resp.headers = nexe_headers
                    return resp
                if zerovm_stderr:
                    self.logger.warning('zerovm stderr: '+zerovm_stderr)
                report = zerovm_stdout.splitlines()
                if len(report) < 5:
                    nexe_validation = 0
                    nexe_retcode = 0
                    nexe_etag = ''
                    nexe_cdr_line = '0 0 0 0 0 0 0 0 0 0 0 0'
                    nexe_status = 'Zerovm crashed'
                else:
                    nexe_validation = int(report[0])
                    nexe_retcode = int(report[1])
                    nexe_etag = report[2]
                    nexe_cdr_line = report[3]
                    nexe_status = '\n'.join(report[4:])

                self.logger.info('Zerovm CDR: %s' % nexe_cdr_line)
                response = Response(app_iter=outputiter,
                    request=req, conditional_response=True)
                response.headers['x-nexe-retcode'] = nexe_retcode
                response.headers['x-nexe-status'] = nexe_status
                response.headers['x-nexe-etag'] = nexe_etag
                response.headers['x-nexe-validation'] = nexe_validation
                response.headers['X-Timestamp'] =\
                    normalize_timestamp(time.time())
                content_length = 0
                for fn in fn_list:
                    content_length += self.os_interface.path.getsize(fn)
                response.content_length = content_length
                response.headers['x-nexe-stdsize'] = ' '.join([str(self.os_interface.path.getsize(fn)) for fn in fn_list])
                if 'x-nexe-content-type' in req.headers:
                    response.headers['Content-Type'] = req.headers['x-nexe-content-type']
                if nexe_name:
                    response.headers['x-node-name'] = nexe_name[0]
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
                elif req.method == 'PUT' and 'x-validator-exec' in req.headers:
                    def validate_resp(status, response_headers, exc_info=None):
                        if 200 <= int(status.split(' ')[0]) < 300:
                            valid = self.validate(req)
                            response_headers.append(('x-nexe-validation',str(valid)))
                        return start_response(status, response_headers, exc_info)
                    return self.app(env, validate_resp)
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

    def validate(self, req):
        validator = str(req.headers.get('x-validator-exec', ''))
        if 'fuzzy' in validator:
            self.zerovm_exename.append('-z')
        else:
            return 0
        try:
            (device, partition, account, container, obj) =\
                split_path(unquote(req.path), 5, 5, True)
        except ValueError:
            return 0
        if self.app.mount_check and not check_mount(self.app.devices, device):
            return 0
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
            nexe_size = os.path.getsize(file.data_file)
        except (DiskFileError, DiskFileNotExist):
            return 0
        if nexe_size > self.zerovm_maxnexe:
            return 0
        if file.is_deleted():
            return 0
        with file.mkstemp() as (zerovm_inputmnfst_fd,
                                zerovm_inputmnfst_fn):
            zerovm_inputmnfst = (
                'Version=%s\n'
                'Nexe=%s\n'
                % (
                    self.zerovm_manifest_ver,
                    file.data_file,
                    ))
            while zerovm_inputmnfst:
                written = self.os_interface.write(zerovm_inputmnfst_fd,
                    zerovm_inputmnfst)
                zerovm_inputmnfst = zerovm_inputmnfst[written:]

            thrd = self.zerovm_thrdpool.spawn(self.execute_zerovm, zerovm_inputmnfst_fn)
            (zerovm_retcode, zerovm_stdout, zerovm_stderr) = thrd.wait()
            if zerovm_retcode:
                err = 'ERROR OBJ.QUERY retcode=%s, '\
                      ' zerovm_stdout=%s'\
                % (self.retcode_map[zerovm_retcode],
                   zerovm_stdout)
                self.logger.exception(err)
                return 0
            if zerovm_stderr:
                self.logger.warning('zerovm stderr: '+zerovm_stderr)
            report = zerovm_stdout.splitlines()
            if len(report) < 5:
                return 0
            else:
                nexe_validation_status = int(report[0])
                metadata = file.metadata
                metadata['Validation-Status'] = nexe_validation_status
                write_metadata(file.data_file, metadata)
                return nexe_validation_status

def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def obj_query_filter(app):
        return ObjectQueryMiddleware(app, conf)
    return obj_query_filter
