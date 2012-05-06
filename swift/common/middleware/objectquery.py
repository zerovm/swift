# needed for etag validation using md5
import re
# for capturing zerovm stdout and stderr
import os
import time
import traceback

# needed for spawning zerovm processes
from subprocess import Popen, STDOUT, PIPE

# needed to limit the number of simultaneously running zerovms
from eventlet import GreenPool, sleep
from eventlet.timeout import Timeout

# needed for parsing manifest files returned from zerovm
from string import split
from urllib import unquote
from hashlib import md5
from webob import Request, Response
from tempfile import mkstemp

# needed for error handling
from webob.exc import HTTPBadRequest, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPRequestTimeout, HTTPUnprocessableEntity, \
    HTTPServiceUnavailable, HTTPLengthRequired, HTTPRequestEntityTooLarge, \
    HTTPInternalServerError

from swift.common.utils import normalize_timestamp, \
    fallocate, split_path, drop_buffer_cache, \
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

        self.devices = conf.get('devices', '/srv/node/')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
            ('true', 't', '1', 'on', 'yes', 'y')
        self.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.log_requests = conf.get('log_requests', 't')[:1].lower() == 't'
        
        self.zerovm_exename = set(i.strip() for i in conf.get('zerovm_exename', 'zerovm1').split() if i.strip())
        self.zerovm_xparams = set(i.strip() for i in conf.get('zerovm_xparams', '-Y2').split() if i.strip())
        # for debug only, harmless
        # self.zerovm_xparams = set(i.strip() for i in conf.get('zerovm_xparams', 'ok. 0').split() if i.strip())
        self.zerovm_nexe_xparams = set(i.strip() for i in conf.get('zerovm_nexe_xparams', '').split() if i.strip())
        
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
        
        # maximum nexe size
        self.zerovm_maxnexe = int(conf.get('zerovm_maxnexe', 256 * 1048576))
        
        # maximum output data file size
        self.zerovm_maxoutput = int(conf.get('zerovm_maxoutput', 64 * 1048576)) # TODO this breaks some tests
        
        self.zerovm_maxchunksize = int(conf.get('zerovm_maxchunksize', 1024 * 1024))
        self.fault_injection = conf.get('fault_injection', '') # for unit-tests.
        self.os_interface = os
        
        # green thread for zerovm execution
        self.zerovm_thrdpool = GreenPool(self.zerovm_maxpool)
        
    def QUERY(self, req):
        """Handle HTTP QUERY requests for the Swift Object Server."""

        try:
            (device, partition, account, container, obj) = \
                split_path(unquote(req.path), 5, 5, True)
        except ValueError, err:
            return HTTPBadRequest(body=str(err), request=req,
                                  content_type='text/plain')

        # TODO log the path and the components on loglevel_debug

        if self.zerovm_thrdpool.free() <= 0 \
            and self.zerovm_thrdpool.waiting() >= self.zerovm_maxqueue:
            return HTTPServiceUnavailable(body='Slot not available',
                    request=req, content_type='text/plain')
        if self.mount_check and not check_mount(self.devices, device):

            # TODO: not consistent return of error code, copied from GET

            return Response(status='507 %s is not mounted' % device)
        if 'content-length' not in req.headers \
            and req.headers.get('transfer-encoding') != 'chunked':
            return HTTPLengthRequired(request=req)
        if 'content-length' in req.headers \
            and int(req.headers['content-length']) \
            > self.zerovm_maxnexe:
            return HTTPRequestEntityTooLarge(body='Your request is too large.'
                    , request=req, content_type='text/plain')
        if 'Content-Type' not in req.headers:
            return HTTPBadRequest(request=req, content_type='text/plain'
                                  , body='No content type')
        if req.headers['Content-Type'] != 'application/octet-stream':
            return HTTPBadRequest(request=req,
                                  body='Invalid Content-Type',
                                  content_type='text/plain')
        file = DiskFile(
            self.devices,
            device,
            partition,
            account,
            container,
            obj,
            self.logger,
            disk_chunk_size=self.disk_chunk_size,
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
            upload_expiration = time.time() + self.max_upload_time
            for chunk in iter(lambda : reader(self.network_chunk_size),
                              ''):
                upload_size += len(chunk)
                if time.time() > upload_expiration:
                    return HTTPRequestTimeout(request=req)
                etag.update(chunk)
                while chunk:
                    written = os.write(zerovm_nexe_fd, chunk)
                    chunk = chunk[written:]
            if 'content-length' in req.headers \
                and int(req.headers['content-length']) != upload_size:
                return Response(status='499 Client Disconnect')
            etag = etag.hexdigest()
            if 'etag' in req.headers and req.headers['etag'].lower() \
                != etag:
                return HTTPUnprocessableEntity(request=req)

            def file_iter(fd, fn):
                """Returns an iterator over the data file."""

                try:
                    chunk_size = file.disk_chunk_size
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
                        self.os_interface.close(fd)
                    except OSError:
                        pass
                    try:
                        self.os_interface.unlink(fn)
                    except OSError:
                        pass

            try:
                # outputiter is responsible to delete temp file
                (zerovm_output_fd, zerovm_output_fn) = \
                    mkstemp(dir=file.tmpdir)
                fallocate(zerovm_output_fd, self.zerovm_maxoutput)
                outputiter = file_iter(zerovm_output_fd,
                        zerovm_output_fn)
            except:
                try: 
                    self.os_interface.close(zerovm_output_fd)
                except OSError: 
                    pass 
                try: 
                    self.os_interface.unlink(zerovm_output_fn) 
                except OSError: 
                    pass 
                raise 
            with file.mkstemp() as (zerovm_outputmnfst_fd,
                                    zerovm_outputmnfst_fn):
                with file.mkstemp() as (zerovm_inputmnfst_fd,
                        zerovm_inputmnfst_fn):
                    zerovm_input_fn = file.data_file
                    zerovm_inputmnfst_part1 = (
                            'version            =11nov2011\n'
                            'zerovm             =%s\n'
                            'nexe               =%s\n'
                            'maxnexe            =%s\n'
                            'input              =%s\n'
                            'maxinput           =%s\n'
                            '#etag              =%s\n'
                            '#content-type      =%s\n'
                            '#x-timestamp       =%s\n'
                             % (
                        self.zerovm_exename,
                        zerovm_nexe_fn,
                        self.zerovm_maxnexe,
                        zerovm_input_fn,
                        self.zerovm_maxinput,
                        file.metadata['ETag'],
                        file.metadata['Content-Type'],
                        file.metadata['X-Timestamp'],
                        ))
                    zerovm_inputmnfst_part2 = ''
                    for (key, value) in file.metadata.iteritems():
                        if key.lower().startswith('x-object-meta-'):
                            zerovm_inputmnfst_part2 += \
                                '#x-data-attr       =' + key[14:] + ':' \
                                + value + '\n'
                    for header in req.headers:
                        if header.lower().startswith('x-object-meta-'):
                            zerovm_inputmnfst_part2 += \
                                '#x-nexe-attr       =' + header[14:] \
                                + ':' + req.headers[header] + '\n'
                    zerovm_inputmnfst_part3 = (
                            'input_mnfst        =%s\n'
                            'output             =%s\n'
                            'maxoutput          =%s\n'
                            'output_mnfst       =%s\n'
                            'maxmnfstline       =%s\n'
                            'maxmnfstlines      =%s\n'
                            'timeout            =%s\n'
                            'kill_timeout       =%s\n'
                            '?zerovm_retcode    =required\n'
                            '?zerovm_status     =required\n'
                            '?etag              =required\n'
                            '?#retcode          =optional\n'
                            '?#status           =optional\n'
                            '?#content-type     =optional\n'
                            '?#x-data-attr      =optional\n'
                            '?#x-nexe-attr      =optional\n'
                             % (
                        zerovm_inputmnfst_fn,
                        zerovm_output_fn,
                        self.zerovm_maxoutput,
                        zerovm_outputmnfst_fn,
                        self.zerovm_maxmnfstline,
                        self.zerovm_maxmnfstlines,
                        self.zerovm_timeout,
                        self.zerovm_kill_timeout,
                        ))
                    zerovm_inputmnfst = zerovm_inputmnfst_part1 \
                        + zerovm_inputmnfst_part2 \
                        + zerovm_inputmnfst_part3
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
                        if len(self.zerovm_nexe_xparams) > 0:
                            cmdline += self.zerovm_nexe_xparams
                        proc = Popen(cmdline, stdout=PIPE,
                                stderr=STDOUT)

                        start = time.time()
                        while time.time() - start < self.zerovm_timeout:
                            if proc.poll() is not None:
                                return (0, 'normal completion:%s'
                                        % proc.communicate()[0])
                            sleep(0.1)
                        if proc.poll() is None:
                            proc.terminate()
                            start = time.time()
                            while time.time() - start \
                                < self.zerovm_kill_timeout:
                                if proc.poll() is not None:
                                    return (1, 'terminated on timeout:%s'
                                             % proc.communicate()[0])
                                sleep(0.1) 
                            proc.kill() 
                            return (2, 'killed on timeout:%s' 
                                    % proc.communicate()[0]) 

                    thrd = self.zerovm_thrdpool.spawn(ex_zerovm)
                    (exor_retcode, exor_status) = thrd.wait()
                    if exor_retcode:
                        raise Exception('ERROR OBJ.QUERY exor_retcode=%s, ' \
                                'exor_status=%s' % (exor_retcode, exor_status))
                    os.lseek(zerovm_outputmnfst_fd, 0, os.SEEK_SET)
                    

                    if os.stat(zerovm_inputmnfst_fn).st_size > \
                        (self.zerovm_maxmnfstline * self.zerovm_maxmnfstlines):
                        raise Exception("Input manifest must be smaller"
                            " than %d." % (self.zerovm_maxmnfstline
                                           * self.zerovm_maxmnfstlines))
                    zerovm_outputmnfst = \
                        split(os.read(zerovm_outputmnfst_fd,
                              self.zerovm_maxmnfstline
                              * self.zerovm_maxmnfstlines), '\n',
                              self.zerovm_maxmnfstlines)

                    def retrieve_mnfst_field(
                        i,
                        n,
                        optional=False,
                        isint=True,
                        rgx=None,
                        ):
                        if not (zerovm_outputmnfst[i])[0:19] == n:
                            if optional:
                                return None
                            else:
                                raise Exception('omnfst: expecting %s got %s,' \
                                        ' retcode=%s, status=>%s' % (n,
                                        (zerovm_outputmnfst[i])[0:19],
                                        exor_retcode, exor_status))
                        if not zerovm_outputmnfst[i][19] == '=': 
                            raise Exception('omnfst: expecting = at %s got %s'
                                    ' retcode=%s, status=>%s' % (n,
                                    zerovm_outputmnfst[i][19],
                                    exor_retcode, exor_status))
                        v = (zerovm_outputmnfst[i])[20:]
                        if isint:
                            v = int(v)
                        if rgx:
                            if not re.match(rgx, v):
                                raise Exception('mnfst: %s does not match %s' 
                                         % (n, rgx))
                        return v

                    zerovm_retcode = retrieve_mnfst_field(0,
                            'zerovm_retcode     ')
                    zerovm_status = retrieve_mnfst_field(1,
                            'zerovm_status      ', isint=False)
                    if zerovm_retcode:
                        raise Exception('ERROR OBJ.QUERY zerovm_retcode=%s,' \
                                ' zerovm_status=%s' % (zerovm_retcode,  
                                zerovm_status))
                    etag = retrieve_mnfst_field(2, 'etag               '
                            , isint=False, rgx=r"([a-fA-F\d]{32})")
                    nexe_retcode = retrieve_mnfst_field(3,
                            '#retcode           ', optional=True)
                    nexe_status = retrieve_mnfst_field(4,
                            '#status            ', optional=True,
                            isint=False)
                    nexe_content_type = retrieve_mnfst_field(5,
                            '#content-type      ', optional=True,
                            isint=False)
                    i = 6
                    response = Response(app_iter=outputiter,
                            request=req, conditional_response=True)
                    while True:
                        nexe_meta = retrieve_mnfst_field(i,
                                '#x-data-attr       ', optional=True,
                                isint=False)
                        if nexe_meta:
                            i += 1
                            (name, val) = split(nexe_meta, ':')
                            response.headers['X-Object-Meta-' + name] = \
                                val
                        else:
                            break
                    response.headers['x-query-nexe-retcode'] = \
                        nexe_retcode
                    response.headers['x-query-nexe-status'] = \
                        nexe_status
                    response.headers['etag'] = etag
                    response.headers['X-Timestamp'] = \
                        normalize_timestamp(time.time())
                    response.content_length = \
                        os.path.getsize(zerovm_output_fn)
                    response.headers['Content-Type'] = nexe_content_type
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
                if hasattr(self, req.method):
                    res = getattr(self, req.method)(req)
                else:
                    return self.app(env, start_response)
            except (Exception, Timeout):
                self.logger.exception(_('ERROR __call__ error with %(method)s'
                    ' %(path)s '), {'method': req.method, 'path': req.path})
                res = HTTPInternalServerError(body=traceback.format_exc())
        trans_time = time.time() - start_time
        if self.log_requests:
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