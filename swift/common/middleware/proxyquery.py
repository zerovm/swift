from __future__ import with_statement
import ctypes
import re
import struct
from eventlet.green import socket
import time
from hashlib import md5
from eventlet import GreenPile, GreenPool
import greenlet
from swift.common.client import quote

try:
    import simplejson as json
except ImportError:
    import json
import uuid
from urllib import unquote
from webob import Request, Response
from random import shuffle, randrange
from eventlet.timeout import Timeout

from webob.exc import HTTPNotFound, HTTPPreconditionFailed, \
    HTTPRequestTimeout, HTTPRequestEntityTooLarge, HTTPBadRequest, HTTPUnprocessableEntity, HTTPServiceUnavailable

from swift.common.utils import split_path, get_logger, TRUE_VALUES, get_remote_client
from swift.proxy.server import update_headers, Controller, ObjectController, delay_denial, ContainerController, AccountController
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ConnectionTimeout, ChunkReadTimeout, \
    ChunkWriteTimeout
from swift.common.constraints import check_utf8

ACCESS_WRITABLE = 0x1
ACCESS_RANDOM = 0x2
ACCESS_NETWORK = 0x4
ACCESS_CDR = 0x8

device_map = {
    'stdin': 0, 'stdout': ACCESS_WRITABLE, 'stderr': ACCESS_WRITABLE,
    'input': ACCESS_RANDOM, 'output': ACCESS_RANDOM | ACCESS_WRITABLE,
    'debug': ACCESS_NETWORK, 'image': ACCESS_CDR
    }


class SequentialResponseBody(object):

    def __init__(self, response, size):
        self.response = response
        self.pos = 0
        self.size = size
        self.max_transfer = self.size.pop(0)

    def read(self, size):
        try:
            if self.pos + size > self.max_transfer:
                size = self.max_transfer - self.pos
            self.pos += size
            self.response.bytes_transferred += size
            return self.response.read(size)
        except Exception:
            raise

    def next_response(self):
        if self.size:
            self.pos = 0
            self.max_transfer = self.size.pop(0)

    def get_content_length(self):
        return self.max_transfer


class ProxyQueryMiddleware(object):

    def __init__(self, app, conf, logger=None):
        self.app = app
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger(conf, log_route='proxy-query')
        self.app.zerovm_maxnexe = int(conf.get('zerovm_maxnexe', 256 * 1048576))
        self.app.zerovm_maxiops = int(conf.get('zerovm_maxiops', 1024 * 1048576))
        self.app.zerovm_maxoutput = int(conf.get('zerovm_maxoutput', 1024 * 1048576))
        self.app.zerovm_maxinput = int(conf.get('zerovm_maxinput', 1024 * 1048576))
        self.app.zerovm_ns_hostname = conf.get('zerovm_ns_hostname')
        self.app.zerovm_ns_maxpool = int(conf.get('zerovm_ns_maxpool', 1000))
        self.app.zerovm_ns_thrdpool = GreenPool(self.app.zerovm_ns_maxpool)

    def __call__(self, env, start_response):
        req = Request(env)
        if req.method == 'POST' and 'x-zerovm-execute' in req.headers:
            if req.content_length and req.content_length < 0:
                return HTTPBadRequest(request=req,
                    body='Invalid Content-Length')
            try:
                version, account, container, obj = split_path(req.path, 1, 4, True)
                path_parts = dict(version=version,
                    account_name=account,
                    container_name=container,
                    object_name=obj)
                if account and not container and not obj:
                    controller = ClusterController(self.app, account)
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
            handler = controller.zerovm_query
            if 'swift.authorize' in req.environ:
                # We call authorize before the handler, always. If authorized,
                # we remove the swift.authorize hook so isn't ever called
                # again. If not authorized, we return the denial unless the
                # controller's method indicates it'd like to gather more
                # information and try again later.
                resp = req.environ['swift.authorize'](req)
                if not resp:
                    # No resp means authorized, no delayed recheck required.
                    del req.environ['swift.authorize']
                else:
                    # Response indicates denial, but we might delay the denial
                    # and recheck later. If not delayed, return the error now.
                    if not getattr(handler, 'delay_denial', None):
                        return resp
            res = handler(req)
        else:
            return self.app(env, start_response)
        return res(env, start_response)


class ZvmNode(object):
    def __init__(self, nid, name, nexe_path, args=None, env=None):
        self.id = nid
        self.name = name
        self.exe = nexe_path
        self.args = args
        self.env = env
        self.channels = []
        self.connect = []
        self.bind = []

    def add_channel(self, device, access, path=None):
        channel = ZvmChannel(device, access, path)
        self.channels.append(channel)

    def add_connections(self, nodes, connect_list):
        for bind_name in connect_list:
            if nodes.get(bind_name):
                bind_node = nodes.get(bind_name)
                if not bind_node is self:
                    bind_node.bind.append(self.name)
                    self.connect.append(bind_name)
            elif nodes.get(bind_name + '1'):
                i = 1
                bind_node = nodes.get(bind_name + str(i))
                while bind_node:
                    if not bind_node is self:
                        bind_node.bind.append(self.name)
                        self.connect.append(bind_name + str(i))
                    i += 1
                    bind_node = nodes.get(bind_name + str(i))
            else:
                raise Exception('Non existing node in connect %s' % bind_name)


class ZvmChannel(object):
    def __init__(self, device, access, path=None):
        self.device = device
        self.access = access
        self.path = path


class ZvmResponse(object):
    def __init__(self, name, status, reason, body, nexe_status, nexe_retcode, nexe_etag):
        self.name = name
        self.status = status
        self.reason = reason
        self.body = body
        self.nexe_status = nexe_status
        self.nexe_retcode = nexe_retcode
        self.nexe_etag = nexe_etag


class ClusterController(Controller):

    server_type = _('Object')

    def __init__(self, app, account_name,
                 **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.nodes = {}

    def get_local_address(self, node):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((node['ip'], node['port']))
        result = s.getsockname()[0]
        s.shutdown(socket.SHUT_RDWR)
        s.close()
        return result

    def get_random_nodes(self):
        partition_count = self.app.object_ring.partition_count()
        part = randrange(0, partition_count)
        nodes = self.app.object_ring.get_part_nodes(part)
        return part, nodes

    def list_account(self, req, account, mask=None, marker=None):
        new_req = req.copy_get()
        if marker:
            new_req.query_string = 'marker=' + marker
        resp = AccountController(self.app, account).GET(new_req)
        if resp.status == 204:
            resp.read()
            return []
        if resp.status < 200 or resp.status >= 300:
            raise Exception('Error querying object server')
        result = data = json.loads(resp.read())
        if not marker:
            while data:
                marker = data[-1]['name']
                data = self.list_account(req, account, None, marker)
                if data:
                    result.extend(data)
            ret = []
            for item in result:
                if mask.match(item['name']):
                    ret.append(item['name'])
            return ret
        return result

    def list_container(self, req, account, container, mask=None, marker=None):
        new_req = req.copy_get()
        new_req.path_info += '/' + quote(container)
        if marker:
            new_req.query_string = 'marker=' + marker
        resp = ContainerController(self.app, account, container).GET(new_req)
        if resp.status == 204:
            resp.read()
            return []
        if resp.status < 200 or resp.status >= 300:
            raise Exception('Error querying object server')
        result = data = json.loads(resp.read())
        if not marker:
            while data:
                marker = data[-1]['name']
                data = self.list_container(req, account, container, None, marker)
                if data:
                    result.extend(data)
            ret = []
            for item in result:
                if mask.match(item['name']):
                    ret.append(item['name'])
            return ret
        return result

    def make_request(self, node, request, ns_port):
        path_info = request.path_info
        top_path = node.channels[0].path
        if top_path:
            path_info += top_path
            account, container, obj = split_path(path_info, 1, 3, True)
            partition, obj_nodes = self.app.object_ring.get_nodes(account, container, obj)
        else:
            partition, obj_nodes = self.get_random_nodes()

        load_from = request.path_info + node.exe
        source_req = request.copy_get()
        source_req.path_info = load_from
        source_req.headers['X-Newest'] = 'true'
        acct, src_container_name, src_obj_name = split_path(load_from, 1, 3, True)
        source_resp = ObjectController(self.app, acct, src_container_name, src_obj_name).GET(source_req)
        if source_resp.status_int >= 300:
            return source_resp
        code_source = source_resp.app_iter
        shuffle(obj_nodes)
        obj_nodes = self.iter_nodes(partition, obj_nodes, self.app.object_ring)
        req = Request.blank(path_info,
            environ=request.environ, headers=request.headers)
        req.content_length = source_resp.content_length
        if req.content_length is None:
            # This indicates a transfer-encoding: chunked source object,
            # which currently only happens because there are more than
            # CONTAINER_LISTING_LIMIT segments in a segmented object. In
            # this case, we're going to refuse request.
            return HTTPRequestEntityTooLarge(request=req)
        req.etag = source_resp.etag
        req.headers['Content-Type'] =\
            source_resp.headers['Content-Type']

        stdlist = []
        for ch in node.channels:
            if 'stdout' in ch.device or 'stderr' in ch.device:
                if ch.path:
                    stdlist.insert(0, ch)
                else:
                    stdlist.append(ch)
        if stdlist:
            req.headers['x-nexe-stdlist'] = ','.join([stdlist[0].device, stdlist[1].device])

        i = 0
        for dst in node.bind:
            dst_id = self.nodes.get(dst).id
            req.headers['x-nexe-channel-' + str(i)] = \
                ','.join(['tcp:' + dst_id + ':0', '/dev/in/' + dst,
                        '0', str(self.app.zerovm_maxiops), str(self.app.zerovm_maxinput), '0,0'])
            i += 1
        for dst in node.connect:
            dst_id = self.nodes.get(dst).id
            req.headers['x-nexe-channel-' + str(i)] = \
            ','.join(['tcp:' + dst_id + ':', '/dev/out/' + dst,
                      '0,0,0', str(self.app.zerovm_maxiops), str(self.app.zerovm_maxoutput)])
            i += 1
        req.headers['x-nexe-channels'] = str(i)

        req.headers['x-node-name'] = node.name + ',' + node.id

        if self.app.zerovm_ns_hostname:
            addr = self.app.zerovm_ns_hostname
        else:
            addr = self.get_local_address(obj_nodes[0])
        req.headers['x-name-service'] = '%s:%d' % (addr, ns_port)
        req.headers['x-nexe-args'] = node.args
        req.headers['x-nexe-env'] = node.env

        def connect():
            for node in obj_nodes:
                req.headers['Expect'] = '100-continue'
                try:
                    with ConnectionTimeout(self.app.conn_timeout):
                        conn = http_connect(node['ip'], node['port'],
                            node['device'], partition, req.method,
                            path_info, headers=req.headers,
                            query_string=req.query_string)
                    with Timeout(self.app.node_timeout):
                        resp = conn.getexpect()
                    if resp.status == 100:
                        conn.node = node
                        return conn
                except:
                    self.exception_occurred(node, _('Object'),
                        _('Expect: 100-continue on %s') % request.path_info)
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

        std_size = [int(s) for s in server_response.getheader('x-nexe-stdsize').split(' ')]
        req.bytes_transferred = 0

        for std in stdlist:
            seq_resp = SequentialResponseBody(server_response, std_size)
            if std.path:
                dest_header = unquote(std.path)
                acct = req.path_info.split('/', 2)[1]
                dest_header = '/' + acct + dest_header
                dest_container_name, dest_obj_name = \
                    dest_header.split('/', 3)[2:]
                dest_req = Request.blank(dest_header)
                dest_req.environ['wsgi.input'] = seq_resp
                dest_req.headers['Content-Length'] = seq_resp.get_content_length()
                dest_resp = \
                    ObjectController(self.app, acct, dest_container_name, dest_obj_name).PUT(dest_req)
                if dest_resp.status_int >= 300:
                    return dest_resp
            else:
                client_response = Response(request=req, conditional_response=True)
                client_response.bytes_transferred = 0

                def file_iter():
                    try:
                        while True:
                            with ChunkReadTimeout(self.app.node_timeout):
                                chunk = seq_resp.read(self.app.object_chunk_size)
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

                client_response.app_iter = file_iter
                update_headers(client_response, server_response.getheaders())
                update_headers(client_response, {'accept-ranges': 'bytes'})
                client_response.status = server_response.status
                client_response.content_length = seq_resp.get_content_length()
                if server_response.getheader('Content-Type'):
                    client_response.charset = None
                    client_response.content_type =\
                    server_response.getheader('Content-Type')
            seq_resp.next_response()
        if not client_response:
            return dest_resp
        return client_response

    def zerovm_query(self, req):
        reader = req.environ['wsgi.input'].read
        upload_size = 0
        upload_expiration = time.time() + self.app.max_upload_time
        etag = md5()
        cluster_config = ''
        for chunk in iter(lambda: reader(self.app.network_chunk_size), ''):
            upload_size += len(chunk)
            if time.time() > upload_expiration:
                return HTTPRequestTimeout(request=req)
            etag.update(chunk)
            cluster_config += chunk

        if 'content-length' in req.headers and\
           int(req.headers['content-length']) != upload_size:
            return Response(status='499 Client Disconnect')
        etag = etag.hexdigest()
        if 'etag' in req.headers and\
           req.headers['etag'].lower() != etag:
            return HTTPUnprocessableEntity(request=req)

        cluster_config = json.loads(cluster_config)
        nid = 0
        for node in cluster_config:
            node_name = node.get('name')
            if not node_name:
                return HTTPBadRequest(request=req, body='Must specify node name')
            nexe_path = node.get('exec').get('path')
            if not nexe_path:
                return HTTPBadRequest(request=req,
                    body='Must specify executable path for %s' % node_name)
            node_count = node.get('count', 1)
            file_list = node.get('file_list')
            read_list = []
            write_list = []
            other_list = []
            if file_list:
                for f in file_list:
                    device = f.get('device')
                    if not device:
                        return HTTPBadRequest(request=req,
                            body='Must specify device for file in %s' % node_name)
                    access = device_map.get(device)
                    if not access:
                        return HTTPBadRequest(request=req,
                            body='Unknown device %s in %s' % (device, node_name))
                    if not access & ACCESS_WRITABLE or access & ACCESS_CDR:
                        if len(read_list) > 0:
                            return HTTPBadRequest(request=req,
                                body='Not more than one readable file per node %s' % node_name)
                        read_list.append(f)
                    elif access & ACCESS_WRITABLE:
                        write_list.append(f)
                    else:
                        other_list.append(f)

                read_group = 0
                for f in read_list:
                    device = f.get('device')
                    access = device_map.get(device)
                    path = f.get('path')
                    if '*' in path:
                        read_group = 1
                        list = []
                        try:
                            container, object = split_path(path, 1, 2, True)
                        except ValueError:
                            return HTTPBadRequest(request=req, body='Invalid path %s' % path)
                        if '*' in container:
                            container = container.replace('*', '.*')
                            mask = re.compile(container)
                            try:
                                containers = self.list_account(req, self.account_name, mask)
                            except Exception:
                                return HTTPBadRequest(request=req, body='Error querying object server')
                            if '*' in object:
                                object = object.replace('*', '.*')
                                mask = re.compile(object)
                            else:
                                mask = None
                            for c in containers:
                                try:
                                    obj_list = self.list_container(req, self.account_name, c, mask)
                                except Exception:
                                    return HTTPBadRequest(request=req,
                                        body='Error querying object server')
                                for obj in obj_list:
                                    list.append('/' + c + '/' + obj)
                        else:
                            object = object.replace('*', '.*')
                            mask = re.compile(object)
                            for obj in self.list_container(req, self.account_name, container, mask):
                                list.append('/' + container + '/' + obj)
                        if not list:
                            return HTTPBadRequest(request=req, body='No objects found in path %s' % path)
                        node_count = len(list)
                        for i in range(1, node_count + 1):
                            new_name = node_name + str(i)
                            new_path = list[i]
                            new_node = self.nodes.get(new_name)
                            if not new_node:
                                new_node = ZvmNode(nid, new_name, nexe_path, node.get('args'), node.get('env'))
                                nid += 1
                                self.nodes[new_name] = new_node
                            new_node.add_channel(device, access, path=new_path)
                    elif path:
                        new_node = self.nodes.get(node_name)
                        if not new_node:
                            new_node = ZvmNode(nid, node_name, nexe_path, node.get('args'), node.get('env'))
                            nid += 1
                            self.nodes[node_name] = new_node
                        new_node.add_channel(device, access, path=path)
                    else:
                        return HTTPBadRequest(request=req, body='Readable file must have a path')

                for f in write_list:
                    device = f.get('device')
                    access = device_map.get(device)
                    path = f.get('path')
                    if '*' in path:
                        if read_group:
                            read_mask = read_list[0].path
                            read_mask = read_mask.replace('*', '(.*)')
                            read_mask = re.compile(read_mask)
                            for i in range(1, node_count + 1):
                                new_name = node_name + str(i)
                                new_path = path
                                new_node = self.nodes.get(new_name)
                                read_path = new_node.channels[0].path
                                m = read_mask.match(read_path)
                                repl = m.group(1)
                                new_path = new_path.replace('*', repl)
                                new_node.add_channel(device, access, path=new_path)
                        else:
                            for i in range(1, node_count + 1):
                                new_name = node_name + str(i)
                                new_path = path
                                new_path.replace('*', new_name)
                                new_node = self.nodes.get(new_name)
                                if not new_node:
                                    new_node = ZvmNode(nid, new_name, nexe_path,
                                        node.get('args'), node.get('env'))
                                    nid += 1
                                    self.nodes[new_name] = new_node
                                new_node.add_channel(device, access, path=new_path)
                    elif path:
                        new_node = self.nodes.get(node_name)
                        if not new_node:
                            new_node = ZvmNode(nid, node_name, nexe_path,
                                node.get('args'), node.get('env'))
                            nid += 1
                            self.nodes[node_name] = new_node
                        new_node.add_channel(device, access, path=path)
                    else:
                        if 'stdout' not in device or 'stderr' not in device:
                            return HTTPBadRequest(request=req,
                                body='Immediate response is not available for device %s' % device)
                        if node_count > 1:
                            for i in range(1, node_count + 1):
                                new_name = node_name + str(i)
                                new_node = self.nodes.get(new_name)
                                if not new_node:
                                    new_node = ZvmNode(nid, new_name, nexe_path,
                                        node.get('args'), node.get('env'))
                                    nid += 1
                                    self.nodes[new_name] = new_node
                                new_node.add_channel(device, access)
                        else:
                            new_node = self.nodes.get(node_name)
                            if not new_node:
                                new_node = ZvmNode(nid, node_name, nexe_path,
                                    node.get('args'), node.get('env'))
                                nid += 1
                                self.nodes[node_name] = new_node
                            new_node.add_channel(device, access)
                for f in other_list:
                    # only debug channel is here, for now
                    device = f.get('device')
                    if not 'debug' in device:
                        return HTTPBadRequest(request=req, body='Bad device name %s' % device)
                    access = device_map.get(device)
                    path = f.get('path')
                    if node_count > 1:
                        for i in range(1, node_count + 1):
                            new_name = node_name + str(i)
                            new_node = self.nodes.get(new_name)
                            if not new_node:
                                new_node = ZvmNode(nid, new_name, nexe_path,
                                    node.get('args'), node.get('env'))
                                nid += 1
                                self.nodes[new_name] = new_node
                            new_node.add_channel(device, access, path=path)
                    else:
                        new_node = self.nodes.get(node_name)
                        if not new_node:
                            new_node = ZvmNode(nid, node_name, nexe_path,
                                node.get('args'), node.get('env'))
                            nid += 1
                            self.nodes[node_name] = new_node
                        new_node.add_channel(device, access, path=path)

        for node in cluster_config:
            connect = node.get('connect')
            if not connect:
                continue
            node_name = node.get('name')
            if self.nodes.get(node_name):
                connect_node = self.nodes.get(node_name)
                try:
                    connect_node.add_connections(self.nodes, connect)
                except Exception:
                    return HTTPBadRequest(request=req,
                        body='Invalid connect string for node %s' % node_name)
            elif self.nodes.get(node_name + '1'):
                j = 1
                connect_node = self.nodes.get(node_name + str(j))
                while connect_node:
                    try:
                        connect_node.add_connections(self.nodes, connect)
                    except Exception:
                        return HTTPBadRequest(request=req,
                            body='Invalid connect string for node %s' % connect_node.name)
                    j += 1
                    connect_node = self.nodes.get(node_name + str(j))
            else:
                return HTTPBadRequest(request=req,
                    body='Non existing node in connect string for node %s' % node_name)

        def ns_server_bind():
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', 0))
            return s.getsockname()[1], s

        def ns_server_run(sock, peers):
            bind_map = {}
            conn_map = {}
            peer_map = {}
            while 1:
                try:
                    message, address = sock.recvfrom(65535)
                    offset = 0
                    alias = struct.unpack_from('!I', message, offset)[0]
                    offset += 4
                    count = struct.unpack_from('!I', message, offset)[0]
                    offset += 4
                    for i in range(count):
                        h, port = struct.unpack_from('!IH', message, offset)[0:2]
                        bind_map.setdefault(alias, {})[h] = port
                        offset += 6
                    conn_map[alias] = ctypes.create_string_buffer(message[offset:])
                    peer_map.setdefault(alias, {})[0] = address[0]
                    peer_map.setdefault(alias, {})[1] = address[1]

                    if len(peer_map) == peers:
                        for src in peer_map.iterkeys():
                            reply = conn_map[src]
                            offset = 0
                            count = struct.unpack_from('!I', reply, offset)[0]
                            offset += 4
                            for i in range(count):
                                h = struct.unpack_from('!I', reply, offset)[0]
                                port = bind_map[h][src]
                                struct.pack_into('!4sH', reply, offset + 4,
                                    socket.inet_pton(socket.AF_INET, peer_map[src][0]), port)
                                offset += 10
                            sock.sendto(reply, (peer_map[src][0], peer_map[src][1]))
                except greenlet.GreenletExit:
                    return

        def ns_server_stop(thr, sock):
            thr.kill()
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()

        ns_port, ns_socket = ns_server_bind()
        if self.app.zerovm_ns_thrdpool.free() <= 0:
            return HTTPServiceUnavailable(body='Cluster slot not available',
                request=req, content_type='text/plain')
        ns_thrd = self.app.zerovm_ns_thrdpool.spawn(ns_server_run, ns_socket, len(self.nodes))

        pile = GreenPile(len(self.nodes))
        for node in self.nodes:
            pile.spawn(self.make_request, node, req, ns_port)
        for resp in pile:
            node_name = resp.getheader('x-node-name')
            nexe_status = resp.getheader('x-nexe-status')
            nexe_retcode = resp.getheader('x-nexe-retcode')
            nexe_etag = resp.getheader('x-nexe-etag')
            node = self.nodes.get(node_name)
            if node:
                node.resp = ZvmResponse(node_name, resp.status, resp.reason, resp.read(),
                    nexe_status, nexe_retcode, nexe_etag)
        resp_list = []
        for node in self.nodes:
            if node.resp:
                resp_list.append(node.resp)
            else:
                resp_list.append(ZvmResponse(node.name, 503, '', '','','',''))
        final_response = Response(request=req, content_type='application/json')
        final_response.body = json.dumps(resp_list)
        ns_server_stop(ns_thrd, ns_socket)
        return final_response


#class QueryController(Controller):
#    """WSGI controller for object requests."""
#
#    server_type = _('Object')
#
#    def __init__(self, app, account_name, container_name, object_name,
#                 **kwargs):
#        Controller.__init__(self, app)
#        self.account_name = unquote(account_name)
#        self.container_name = unquote(container_name)
#        self.object_name = unquote(object_name)
#
#    @delay_denial
#    def zerovm_query(self, req):
#        """Handler for HTTP QUERY requests."""
#        # TODO: log the path and the components on loglevel_debug.
#
#        if 'swift.authorize' in req.environ:
#            req.acl = \
#                self.container_info(self.account_name, self.container_name)[2]
#            aresp = req.environ['swift.authorize'](req)
#            if aresp:
#                return aresp
#
#        source_header = req.headers.get('X-Load-From')
#        if source_header:
#            source_resp = None
#            source_header = unquote(source_header)
#            acct = req.path_info.split('/', 2)[1]
#            if not source_header.startswith('/'):
#                source_header = '/' + source_header
#            source_header = '/' + acct + source_header
#            try:
#                src_container_name, src_obj_name = \
#                    source_header.split('/', 3)[2:]
#            except ValueError:
#                return HTTPPreconditionFailed(request=req,
#                    body='X-Load-From header must be of the form'
#                    '<container name>/<object name>')
#            source_req = req.copy_get()
#            source_req.path_info = source_header
#            source_req.headers['X-Newest'] = 'true'
#            source_resp = ObjectController(self.app, acct, src_container_name, src_obj_name).GET(source_req)
#            if source_resp.status_int >= 300:
#                return source_resp
#            new_req = Request.blank(req.path_info,
#                        environ=req.environ, headers=req.headers)
#            code_source = source_resp.app_iter
#            new_req.content_length = source_resp.content_length
#            if new_req.content_length is None:
#                # This indicates a transfer-encoding: chunked source object,
#                # which currently only happens because there are more than
#                # CONTAINER_LISTING_LIMIT segments in a segmented object. In
#                # this case, we're going to refuse request.
#                return HTTPRequestEntityTooLarge(request=req)
#            new_req.etag = source_resp.etag
#            # we no longer need the X-Load-From header
#            del new_req.headers['X-Load-From']
#            new_req.headers['Content-Type'] = \
#                source_resp.headers['Content-Type']
#            if new_req.headers.get('x-fresh-metadata', 'false').lower() \
#                    not in TRUE_VALUES:
#                for k, v in source_resp.headers.items():
#                    if k.lower().startswith('x-object-meta-'):
#                        new_req.headers[k] = v
#                for k, v in req.headers.items():
#                    if k.lower().startswith('x-object-meta-'):
#                        new_req.headers[k] = v
#            req = new_req
#        else:
#            reader = req.environ['wsgi.input'].read
#            code_source = iter(lambda: reader(self.app.client_chunk_size), '')
#
#        partition, nodes = self.app.object_ring.get_nodes(
#            self.account_name, self.container_name, self.object_name)
#        shuffle(nodes)
#        nodes = self.iter_nodes(partition, nodes, self.app.object_ring)
#
#        def connect():
#            for node in nodes:
#                req.headers['Expect'] = '100-continue'
#                try:
#                    with ConnectionTimeout(self.app.conn_timeout):
#                        conn = http_connect(node['ip'], node['port'],
#                            node['device'], partition, req.method,
#                            req.path_info, headers=req.headers,
#                            query_string=req.query_string)
#                    with Timeout(self.app.node_timeout):
#                        resp = conn.getexpect()
#                    if resp.status == 100:
#                        conn.node = node
#                        return conn
#                except:
#                    self.exception_occurred(node, _('Object'),
#                        _('Expect: 100-continue on %s') % req.path_info)
#                    continue
#        conn = connect()
#        if not conn:
#            raise Exception('Cannot find suitable node to execute code on')
#        chunked = req.headers.get('transfer-encoding')
#        try:
#            req.bytes_transferred = 0
#            while True:
#                with ChunkReadTimeout(self.app.node_timeout):
#                    try:
#                        chunk = next(code_source)
#                    except StopIteration:
#                        if chunked:
#                            conn.send('0\r\n\r\n')
#                        break
#                    req.bytes_transferred += len(chunk)
#                    if req.bytes_transferred > self.app.zerovm_maxnexe:
#                        return HTTPRequestEntityTooLarge(request=req)
#                    try:
#                        with ChunkWriteTimeout(self.app.node_timeout):
#                            conn.send('%x\r\n%s\r\n' % (len(chunk), chunk)
#                                if chunked else chunk)
#                    except (Exception, ChunkWriteTimeout):
#                        raise Exception(conn.node, _('Object'),
#                            _('Trying to write to %s') % req.path_info)
#
#        except ChunkReadTimeout, err:
#            self.app.logger.warn(
#                _('ERROR Client read timeout (%ss)'), err.seconds)
#            return HTTPRequestTimeout(request=req)
#        except Exception:
#            req.client_disconnect = True
#            self.app.logger.exception(
#                _('ERROR Exception causing client disconnect'))
#            return Response(status='499 Client Disconnect')
#        if req.content_length and req.bytes_transferred < req.content_length:
#            req.client_disconnect = True
#            self.app.logger.warn(
#                _('Client disconnected without sending enough data'))
#            return Response(status='499 Client Disconnect')
#        try:
#            with Timeout(self.app.node_timeout):
#                server_response = conn.getresponse()
#        except (Exception, Timeout):
#            self.exception_occurred(conn.node, _('Object'),
#                    _('Trying to get final status of PUT to %s')
#                     % req.path_info)
#            return Response(status='499 Client Disconnect')
#        if server_response.status != 200:
#            raise Exception('Error querying object server')
#        client_response = Response(request=req, conditional_response=True)
#        client_response.bytes_transferred = 0
#
#        def file_iter():
#            try:
#                while True:
#                    with ChunkReadTimeout(self.app.node_timeout):
#                        chunk = server_response.read(self.app.object_chunk_size)
#                    if not chunk:
#                        break
#                    yield chunk
#                    client_response.bytes_transferred += len(chunk)
#            except GeneratorExit:
#                client_response.client_disconnect = True
#                self.app.logger.warn(_('Client disconnected on read'))
#            except (Exception, Timeout):
#                self.exception_occurred(conn.node, _('Object'),
#                    _('Trying to read during QUERY of %s') % req.path_info)
#                raise
#        if source_header:
#            # reset the bytes, since the user didn't actually send anything
#            req.bytes_transferred = 0
#        dest_header = req.headers.get('X-Store-To')
#        if dest_header and server_response.status == 200:
#            dest_header = unquote(dest_header)
#            acct = req.path_info.split('/', 2)[1]
#            if not dest_header.startswith('/'):
#                dest_header = '/' + dest_header
#            dest_header = '/' + acct + dest_header
#            try:
#                dest_container_name, dest_obj_name = \
#                    dest_header.split('/', 3)[2:]
#            except ValueError:
#                return HTTPPreconditionFailed(request=req,
#                    body='X-Store-To header must be of the form'
#                    '<container name>/<object name>')
#            dest_req = Request.blank(dest_header)
#            dest_req.environ['wsgi.input'] = server_response
#            dest_req.headers['Content-Length'] = \
#                          server_response.getheader('Content-Length')
#            dest_resp = ObjectController(self.app, acct, dest_container_name, dest_obj_name).PUT(dest_req)
#            if dest_resp.status_int >= 300:
#                return dest_resp
#            client_response = dest_resp
#        else:
#            client_response.app_iter = file_iter()
#            update_headers(client_response, server_response.getheaders())
#            update_headers(client_response, {'accept-ranges': 'bytes'})
#            client_response.status = server_response.status
#            client_response.content_length = \
#                            server_response.getheader('Content-Length')
#            if server_response.getheader('Content-Type'):
#                client_response.charset = None
#                client_response.content_type = \
#                            server_response.getheader('Content-Type')
#        return client_response


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def query_filter(app):
        return ProxyQueryMiddleware(app, conf)

    return query_filter
