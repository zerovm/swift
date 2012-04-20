# needed for spawning zerovm processes
from subprocess import Popen, STDOUT, PIPE

# needed to limit the number of simultaneously running zerovms
from eventlet import GreenPool, sleep
from eventlet.timeout import Timeout

# needed for parsing manifest files returned from zerovm
from string import split

# needed for error handling
from webob.exc import HTTPBadRequest, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPRequestTimeout, HTTPUnprocessableEntity, \
    HTTPServiceUnavailable, HTTPLengthRequired, HTTPRequestEntityTooLarge
    
from urllib import unquote
from hashlib import md5
from webob import Request, Response
from tempfile import mkstemp
from random import shuffle

# needed for etag validation using md5
import re

# for capturing zerovm stdout and stderr
import os
import time

from swift.common.utils import normalize_timestamp, \
    fallocate, split_path, drop_buffer_cache, \
    get_logger, TRUE_VALUES
    
from swift.obj.server import ObjectController, DiskFile
from swift.common.constraints import check_mount
from swift.common.exceptions import ConnectionTimeout, DiskFileError, \
    DiskFileNotExist, ChunkReadTimeout, ChunkWriteTimeout
from swift.common.bufferedhttp import http_connect
from swift.proxy.server import update_headers

class QueryMiddleware(object):
    
    def __init__(self, app, conf, logger=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='query')
            
        self.obj = 0
        if 'devices' in conf:
            self.obj = 1
            
    def objQUERY(self, req):
        """Handle HTTP QUERY requests for the Swift Object Server."""

        

        def initvar(name, value):
            ''' initialize class configuration vars '''
            # TODO: this generally should be better done in the __init__ method
            # but this is done here temporarily to keep code changes localized
            # additionally these values must be loaded from config file
            if not hasattr(ObjectController, name):
                setattr(ObjectController, name, value)

        # names of executables for zerovm
        initvar('zerovm_exename', ['zerovm1'])

        # additional params for ZeroVM
        initvar('zerovm_xparams', ['-Y2'])

        # additional params for NEXE
        initvar('zerovm_nexe_xparams', [])

        # initvar('zerovm_xparams', ['ok.','0'])  # for debug only, harmless

        # maximum number of simultaneous running zerovms, others are queued
        initvar('zerovm_maxpool', 2)

        # maximum length of queue of request awaiting zerovm executions
        initvar('zerovm_maxqueue', 3)

        # timeout for zerovm to finish execution
        initvar('zerovm_timeout', 5)

        # timeout for zerovm between TERM signal and KILL signal
        initvar('zerovm_kill_timeout', 1)

        # maximum length of manifest line (both input and output)
        initvar('zerovm_maxmnfstline', 1024)

        # maximum number of lines in input and output manifest files
        initvar('zerovm_maxmnfstlines', 128)

        # maximum input data file size
        initvar('zerovm_maxinput', 256 * 1048576)

        # maximum nexe size
        initvar('zerovm_maxnexe', 256 * 1048576)

        # maximum output data file size
        initvar('zerovm_maxoutput', 64 * 1048576)  # TODO this breaks some tests
        
        initvar('zerovm_maxchunksize', 1024 * 1024)
        
        initvar('os_interface', os)
        
        # for unit-tests.
        initvar('fault_injection', '')
        
        # green thread for zerovm execution
        initvar('zerovm_thrdpool', GreenPool(self.zerovm_maxpool))
        if self.zerovm_thrdpool.size != self.zerovm_maxpool:
            self.zerovm_thrdpool = GreenPool(self.zerovm_maxpool)

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
                
    def proxyQUERY(self, req):
        """Handler for HTTP QUERY requests."""
        # TODO: log the path and the components on loglevel_debug.
        
        # initialize class configuration vars
        # TODO: this generally should be better done in the __init__ method
        #       but this is done here temporarily to keep code changes localized
        #       additionally these values must be loaded from config file.
        def initvar(name, value):
            if not hasattr(ObjectController,name):
                setattr(ObjectController,name,value)
        
        # Maximum nexe size
        initvar('zerovm_maxnexe', 256*1048576)

        if 'swift.authorize' in req.environ:
            req.acl = \
                self.container_info(self.account_name, self.container_name)[2]
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        source_header = req.headers.get('X-Load-From')
        if source_header:
            source_resp = None
            source_header = unquote(source_header)
            acct = req.path_info.split('/', 2)[1]
            if not source_header.startswith('/'):
                source_header = '/' + source_header
            source_header = '/' + acct + source_header
            try:
                src_container_name, src_obj_name = \
                    source_header.split('/', 3)[2:]
            except ValueError:
                return HTTPPreconditionFailed(request=req,
                    body='X-Load-From header must be of the form'
                    '<container name>/<object name>')
            source_req = req.copy_get()
            source_req.path_info = source_header
            source_req.headers['X-Newest'] = 'true'
            orig_obj_name = self.object_name
            orig_container_name = self.container_name
            self.object_name = src_obj_name
            self.container_name = src_container_name
            source_resp = self.GET(source_req)
            if source_resp.status_int >= 300:
                return source_resp
            self.object_name = orig_obj_name
            self.container_name = orig_container_name
            new_req = Request.blank(req.path_info,
                        environ=req.environ, headers=req.headers)
            code_source = source_resp.app_iter
            new_req.content_length = source_resp.content_length
            if new_req.content_length is None:
                # This indicates a transfer-encoding: chunked source object,
                # which currently only happens because there are more than
                # CONTAINER_LISTING_LIMIT segments in a segmented object. In
                # this case, we're going to refuse request.
                return HTTPRequestEntityTooLarge(request=req)
            new_req.etag = source_resp.etag
            # we no longer need the X-Copy-Load header
            del new_req.headers['X-Load-From']
            new_req.headers['Content-Type'] = \
                source_resp.headers['Content-Type']
            if new_req.headers.get('x-fresh-metadata', 'false').lower() \
                    not in TRUE_VALUES:
                for k, v in source_resp.headers.items():
                    if k.lower().startswith('x-object-meta-'):
                        new_req.headers[k] = v
                for k, v in req.headers.items():
                    if k.lower().startswith('x-object-meta-'):
                        new_req.headers[k] = v
            req = new_req
        else:
            reader = req.environ['wsgi.input'].read
            code_source = iter(lambda: reader(self.app.client_chunk_size), '')
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        shuffle(nodes)
        nodes = self.iter_nodes(partition, nodes, self.app.object_ring)
        def connect():
            for node in nodes:
                req.headers['Expect'] = '100-continue'
                try:
                    with ConnectionTimeout(self.app.conn_timeout):
                        conn = http_connect(node['ip'], node['port'],
                            node['device'], partition, req.method, 
                            req.path_info, headers=req.headers,
                            query_string=req.query_string)
                    with Timeout(self.app.node_timeout):
                        resp = conn.getexpect()
                    if resp.status == 100:
                        conn.node = node
                        return conn
                except:
                    self.exception_occurred(node, _('Object'),
                        _('Expect: 100-continue on %s') % req.path_info)
                    continue
        conn = connect()
        if not conn:
            raise Exception('Cannot find suitable node to execute code on')
        # TODO: http-chunking not suppported yet, but the work is in progress
        chunked = req.headers.get('transfer-encoding') 
        try:
            req.bytes_transferred = 0
            while True:
                with ChunkReadTimeout(self.app.node_timeout):
                    try:
                        chunk = next(code_source)
                    except StopIteration:
                        if chunked: #preparation to support chunking
                            pass
                        break
                    req.bytes_transferred += len(chunk)
                    if req.bytes_transferred > self.zerovm_maxnexe:
                        return HTTPRequestEntityTooLarge(request=req)
                    try:
                        with ChunkWriteTimeout(self.app.node_timeout):
                            conn.send(chunk)
                    except (Exception, ChunkWriteTimeout):
                        raise Exception(conn.node, _('Object'),
                            _('Trying to write to %s') % req.path_info)
                        
        except ChunkReadTimeout, err:
            self.app.logger.warn(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            return HTTPRequestTimeout(request=req)
        except Exception:
            req.client_disconnect = True
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            return Response(status='499 Client Disconnect')
        if req.content_length and req.bytes_transferred < req.content_length:
            req.client_disconnect = True
            self.app.logger.warn(
                _('Client disconnected without sending enough data'))
            return Response(status='499 Client Disconnect')
        try:
            with Timeout(self.app.node_timeout):
                server_response = conn.getresponse()
        except (Exception, Timeout):
            self.exception_occurred(conn.node, _('Object'),
                    _('Trying to get final status of PUT to %s')
                     % req.path_info)
            return Response(status='499 Client Disconnect')
        if server_response.status != 200:
            raise Exception('Error querying object server')
        client_response = Response(request=req, conditional_response=True)
        client_response.bytes_transferred = 0
        def file_iter():
            try:
                while True:
                    with ChunkReadTimeout(self.app.node_timeout):
                        chunk = server_response.read(self.app.object_chunk_size)
                    if not chunk:
                        break
                    yield chunk
                    client_response.bytes_transferred += len(chunk)
            except GeneratorExit:
                client_response.client_disconnect = True
                self.app.logger.warn(_('Client disconnected on read'))
            except (Exception, Timeout):
                self.exception_occurred(conn.node, _('Object'),
                    _('Trying to read during QUERY of %s') % req.path_info)
                raise
        if source_header:
            # reset the bytes, since the user didn't actually send anything
            req.bytes_transferred = 0
        dest_header = req.headers.get('X-Store-To')
        if dest_header and server_response.status == 200:
            dest_header = unquote(dest_header)
            acct = req.path_info.split('/', 2)[1]
            if not dest_header.startswith('/'):
                dest_header = '/' + dest_header
            dest_header = '/' + acct + dest_header
            try:
                dest_container_name, dest_obj_name = \
                    dest_header.split('/', 3)[2:]
            except ValueError:
                return HTTPPreconditionFailed(request=req,
                    body='X-Store-To header must be of the form'
                    '<container name>/<object name>')
            dest_req = Request.blank(dest_header)
            dest_req.environ['wsgi.input'] = server_response
            dest_req.headers['Content-Length'] = \
                          server_response.getheader('Content-Length')
            orig_obj_name = self.object_name
            orig_container_name = self.container_name
            self.object_name = dest_obj_name
            self.container_name = dest_container_name
            dest_resp = self.PUT(dest_req)
            if dest_resp.status_int >= 300:
                return dest_resp
            self.object_name = orig_obj_name
            self.container_name = orig_container_name
            client_response = dest_resp
        else:
            client_response.app_iter = file_iter()
            update_headers(client_response, server_response.getheaders())
            update_headers(client_response, {'accept-ranges': 'bytes'})
            client_response.status = server_response.status
            client_response.content_length = \
                            server_response.getheader('Content-Length')
            if server_response.getheader('Content-Type'):
                client_response.charset = None
                client_response.content_type = \
                            server_response.getheader('Content-Type')    
        return client_response
        
    def __call__(self, env, start_response):
        req = Request(env)
        if req.method == 'QUERY':
            if self.obj == 1:
                return self.objQUERY(req)(env, start_response)
            else:
                return self.proxyQUERY(req)(env, start_response)
        else:
            return self.app(env, start_response)
    
def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def query_filter(app):
        return QueryMiddleware(app, conf)
    return query_filter
