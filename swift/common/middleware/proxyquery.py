import os
import uuid
from urllib import unquote
from webob import Request, Response
from random import shuffle
from eventlet.timeout import Timeout

from webob.exc import HTTPNotFound, HTTPPreconditionFailed, \
    HTTPRequestTimeout, HTTPRequestEntityTooLarge, HTTPBadRequest
    
from swift.common.utils import split_path, get_logger, TRUE_VALUES, get_remote_client
from swift.proxy.server import update_headers, Controller, ObjectController
from swift.common.bufferedhttp import http_connect
from swift.common.ring import Ring
from swift.common.exceptions import ConnectionTimeout, ChunkReadTimeout, \
    ChunkWriteTimeout
from swift.common.constraints import check_utf8
from swift.common.db import DatabaseConnectionError, GreenDBConnection

class ProxyQueryMiddleware(object):
    
    def __init__(self, app, conf, logger=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='proxy-query')
        self.app.zerovm_maxnexe = int(conf.get('zerovm_maxnexe', 256*1048576))

    def __call__(self, env, start_response):
        req = Request(env)
        if req.method == 'POST' and 'x-zerovm-execute' in req.headers :
            if req.content_length and req.content_length < 0:
                return HTTPBadRequest(request=req,
                    body='Invalid Content-Length')
            try:
                version, account, container, obj = split_path(req.path, 1, 4, True)
                path_parts = dict(version=version,
                    account_name=account,
                    container_name=container,
                    object_name=obj)
                controller = QueryController(self.app, account, container, obj)
            except ValueError:
                return HTTPNotFound(request=req)

            if not check_utf8(req.path_info):
                return HTTPPreconditionFailed(request=req, body='Invalid UTF8')
            if not controller:
                return HTTPPreconditionFailed(request=req, body='Bad URL')

            if 'swift.trans_id' not in req.environ:
                # if this wasn't set by an earlier middleware, set it now
                trans_id = 'tx' + uuid.uuid4().hex
                req.environ['swift.trans_id'] = trans_id
                self.logger.txn_id = trans_id
            req.headers['x-trans-id'] = req.environ['swift.trans_id']
            controller.trans_id = req.environ['swift.trans_id']
            self.logger.client_ip = get_remote_client(req)
            if path_parts['version']:
                req.path_info_pop()
            res = controller.zerovm_query(req)
        else:
            return self.app(env, start_response)
        
        return res(env, start_response)


class QueryController(Controller):
    """WSGI controller for object requests."""

    server_type = _('Object')

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)

    def zerovm_query(self, req):
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
            source_resp = ObjectController(self.app, acct, src_container_name, src_obj_name).GET(source_req)
            if source_resp.status_int >= 300:
                return source_resp
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
        chunked = req.headers.get('transfer-encoding')
        try:
            req.bytes_transferred = 0
            while True:
                with ChunkReadTimeout(self.app.node_timeout):
                    try:
                        chunk = next(code_source)
                    except StopIteration:
                        if chunked:
                            conn.send('0\r\n\r\n')
                        break
                    req.bytes_transferred += len(chunk)
                    if req.bytes_transferred > self.app.zerovm_maxnexe:
                        return HTTPRequestEntityTooLarge(request=req)
                    try:
                        with ChunkWriteTimeout(self.app.node_timeout):
                            conn.send('%x\r\n%s\r\n' % (len(chunk), chunk)
                                if chunked else chunk)
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
            dest_resp = ObjectController(self.app, acct, dest_container_name, dest_obj_name).PUT(dest_req)
            if dest_resp.status_int >= 300:
                return dest_resp
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
