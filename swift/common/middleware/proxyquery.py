import os

from urllib import unquote
from webob import Request, Response
from random import shuffle
from eventlet.timeout import Timeout

from webob.exc import HTTPNotFound, HTTPPreconditionFailed, \
    HTTPRequestTimeout, HTTPRequestEntityTooLarge
    
from swift.common.utils import split_path, get_logger, TRUE_VALUES
from swift.proxy.server import update_headers, Controller, ObjectController
from swift.common.bufferedhttp import http_connect
from swift.common.ring import Ring
from swift.common.exceptions import ConnectionTimeout, ChunkReadTimeout, \
    ChunkWriteTimeout

class ProxyQueryMiddleware(object):
    
    def __init__(self, app, conf, logger=None, object_ring=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='proxy-query')
        self.zerovm_maxnexe = int(conf.get('zerovm_maxnexe', 256*1048576))
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.object_ring = object_ring or \
            Ring(os.path.join(swift_dir, 'object.ring.gz'))
        self.client_chunk_size = int(conf.get('client_chunk_size', 65536))
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.object_chunk_size = int(conf.get('object_chunk_size', 65536))
            
    def get_controller(self, path):
        version, account, container, obj = split_path(path, 1, 4, True)
        d = dict(version=version,
                account_name=account,
                container_name=container,
                object_name=obj)
        return QueryController, d
            
    def __call__(self, env, start_response):
        req = Request(env)
        
        try:
            controller, path_parts = self.get_controller(req.path)
        except ValueError:
            return HTTPNotFound(request=req)
        
        controller = controller(self, self.app, **path_parts)
        
        if hasattr(controller, req.method):
            res = getattr(controller, req.method)(req)
        else:
            return self.app(env, start_response)
        
        return res(env, start_response)


class QueryController(Controller):
    """WSGI controller for object requests."""

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)
        self.proxy_controller = ObjectController(self, app, account_name, container_name, object_name)
        
    def QUERY(self, req):
        """Handler for HTTP QUERY requests."""
        # TODO: log the path and the components on loglevel_debug.
        
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
            source_resp = self.proxy_controller.GET(source_req)
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
                    if req.bytes_transferred > self.app.zerovm_maxnexe:
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
            dest_resp = self.proxy_controller.PUT(dest_req)
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
                    
def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def query_filter(app):
        return ProxyQueryMiddleware(app, conf)
    return query_filter
