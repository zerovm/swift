from __future__ import with_statement
import ctypes
import re
import struct
import traceback
from eventlet.green import socket
import time
from hashlib import md5
from eventlet import GreenPile, GreenPool, sleep
import greenlet
from swiftclient.client import quote

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

    def read(self, size=None):
        try:
            if size is None or (self.pos + size > self.max_transfer):
                size = self.max_transfer - self.pos
            self.pos += size
            return self.response.read(size)
        except Exception:
            raise

    def next_response(self):
        if len(self.size) > 0:
            self.pos = 0
            self.max_transfer = self.size.pop(0)

    def get_content_length(self):
        return self.max_transfer

class NameService(object):

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def start(self, pool, peers):
        self.sock.bind(('', 0))
        self.peers = peers
        self.thread = pool.spawn(self.run)
        return self.sock.getsockname()[1]

    def run(self):
        bind_map = {}
        conn_map = {}
        peer_map = {}
        while 1:
            try:
                message, address = self.sock.recvfrom(65535)
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
                peer_map[alias][1] = address[1]

                if len(peer_map) == self.peers:
                    for src in peer_map.iterkeys():
                        reply = conn_map[src]
                        offset = 0
                        count = struct.unpack_from('!I', reply, offset)[0]
                        offset += 4
                        for i in range(count):
                            h = struct.unpack_from('!I', reply, offset)[0]
                            port = bind_map[h][src]
                            struct.pack_into('!4sH', reply, offset,
                                socket.inet_pton(socket.AF_INET, peer_map[src][0]), port)
                            offset += 6
                        self.sock.sendto(reply, (peer_map[src][0], peer_map[src][1]))
            except greenlet.GreenletExit:
                return
            except Exception:
                pass

    def stop(self):
        self.thread.kill()
        self.sock.close()

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

        self.app.max_upload_time = int(conf.get('max_upload_time', 86400))
        self.app.network_chunk_size = int(conf.get('network_chunk_size', 65536))

    def __call__(self, env, start_response):
        req = Request(env)
        if req.method == 'POST' and 'x-zerovm-execute' in req.headers:
            if req.content_length and req.content_length < 0:
                return HTTPBadRequest(request=req,
                    body='Invalid Content-Length')(env, start_response)
            try:
                version, account, container, obj = split_path(req.path, 1, 4, True)
                path_parts = dict(version=version,
                    account_name=account,
                    container_name=container,
                    object_name=obj)
                if account and not container and not obj:
                    controller = self.get_controller(account)
            except ValueError:
                return HTTPNotFound(request=req)(env, start_response)

            if not check_utf8(req.path_info):
                return HTTPPreconditionFailed(request=req, body='Invalid UTF8')(env, start_response)
            if not controller:
                return HTTPPreconditionFailed(request=req, body='Bad URL')(env, start_response)

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
                        return resp(env, start_response)
            res = handler(req)
        else:
            return self.app(env, start_response)
        return res(env, start_response)

    def get_controller(self, account):
        return ClusterController(self.app, account)


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
            elif nodes.get(bind_name + '-1'):
                i = 1
                bind_node = nodes.get(bind_name + '-' + str(i))
                while bind_node:
                    if not bind_node is self:
                        bind_node.bind.append(self.name)
                        self.connect.append(bind_name + '-' + str(i))
                    i += 1
                    bind_node = nodes.get(bind_name + '-' + str(i))
            else:
                raise Exception('Non-existing node in connect %s' % bind_name)


class ZvmChannel(object):
    def __init__(self, device, access, path=None):
        self.device = device
        self.access = access
        self.path = path


class ZvmResponse(object):
    def __init__(self, name, status, body, nexe_status, nexe_retcode, nexe_etag):
        self.name = name
        self.status = status
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
        partition_count = self.app.object_ring.partition_count
        part = randrange(0, partition_count)
        nodes = self.app.object_ring.get_part_nodes(part)
        return part, nodes

    def list_account(self, req, account, mask=None, marker=None):
        new_req = req.copy_get()
        new_req.query_string = 'format=json'
        if marker:
            new_req.query_string += '&marker=' + marker
        resp = AccountController(self.app, account).GET(new_req)
        if resp.status_int == 204:
            data = resp.body
            return []
        if resp.status_int < 200 or resp.status_int >= 300:
            raise Exception('Error querying object server')
        result = data = json.loads(resp.body)
        if not marker:
            while data:
                marker = data[-1]['name']
                data = self.list_account(req, account, None, marker)
                if data:
                    result.extend(data)
            ret = []
            for item in result:
                if not mask or mask.match(item['name']):
                    ret.append(item['name'])
            return ret
        return result

    def list_container(self, req, account, container, mask=None, marker=None):
        new_req = req.copy_get()
        new_req.path_info += '/' + quote(container)
        new_req.query_string = 'format=json'
        if marker:
            new_req.query_string += '&marker=' + marker
        resp = ContainerController(self.app, account, container).GET(new_req)
        if resp.status_int == 204:
            data = resp.body
            return []
        if resp.status_int < 200 or resp.status_int >= 300:
            raise Exception('Error querying object server')
        result = data = json.loads(resp.body)
        if not marker:
            while data:
                marker = data[-1]['name']
                data = self.list_container(req, account, container, None, marker)
                if data:
                    result.extend(data)
            ret = []
            for item in result:
                if not mask or mask.match(item['name']):
                    ret.append(item['name'])
            return ret
        return result

    def make_request(self, node, request, ns_port):
        nexe_headers = {
            'x-node-name': node.name,
            'x-nexe-status': 'ZeroVM did not run',
            'x-nexe-retcode' : 0,
            'x-nexe-etag': ''
        }
        path_info = request.path_info
        top_path = node.channels[0].path
        if top_path \
           and not (node.channels[0].access & ACCESS_WRITABLE) \
        and not (node.channels[0].access & ACCESS_NETWORK):
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
            source_resp.headers = nexe_headers
            return source_resp
        code_source = source_resp.app_iter
        shuffle(obj_nodes)
        obj_nodes_iter = self.iter_nodes(partition, obj_nodes, self.app.object_ring)
        req = Request.blank(path_info,
            environ=request.environ, headers=request.headers)
        req.content_length = source_resp.content_length
        if req.content_length is None:
            # This indicates a transfer-encoding: chunked source object,
            # which currently only happens because there are more than
            # CONTAINER_LISTING_LIMIT segments in a segmented object. In
            # this case, we're going to refuse request.
            return HTTPRequestEntityTooLarge(request=req, headers=nexe_headers)
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
            req.headers['x-nexe-stdlist'] = ','.join([s.device for s in stdlist])

        i = 0
        for dst in node.bind:
            dst_id = self.nodes.get(dst).id
            req.headers['x-nexe-channel-' + str(i)] = \
                ','.join(['tcp:%d:0' % dst_id, '/dev/in/' + dst,
                        '0', str(self.app.zerovm_maxiops), str(self.app.zerovm_maxinput), '0,0'])
            i += 1
        for dst in node.connect:
            dst_id = self.nodes.get(dst).id
            req.headers['x-nexe-channel-' + str(i)] = \
            ','.join(['tcp:%d:' % dst_id, '/dev/out/' + dst,
                      '0,0,0', str(self.app.zerovm_maxiops), str(self.app.zerovm_maxoutput)])
            i += 1
        req.headers['x-nexe-channels'] = str(i)

        req.headers['x-node-name'] = '%s,%d' % (node.name, node.id)

        if self.app.zerovm_ns_hostname:
            addr = self.app.zerovm_ns_hostname
        else:
            for n in obj_nodes:
                addr = self.get_local_address(n)
                if addr:
                    break
        req.headers['x-name-service'] = 'udp:%s:%d' % (addr, ns_port)
        if node.args:
            req.headers['x-nexe-args'] = node.args
        if node.env:
            req.headers['x-nexe-env'] = ','.join(reduce(lambda x, y: x + y, node.env.items()))
        req.path_info = path_info

        def connect():
            for node in obj_nodes_iter:
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
            self.app.logger.exception(
                _('ERROR Cannot find suitable node to execute code on'))
            return HTTPServiceUnavailable(body='Cannot find suitable node to execute code on', headers=nexe_headers)

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
                        return HTTPRequestEntityTooLarge(request=req, headers=nexe_headers)
                    try:
                        with ChunkWriteTimeout(self.app.node_timeout):
                            conn.send('%x\r\n%s\r\n' % (len(chunk), chunk)
                            if chunked else chunk)
                            sleep()
                    except (Exception, ChunkWriteTimeout):
                        raise Exception(conn.node, _('Object'),
                            _('Trying to write to %s') % req.path_info)

        except ChunkReadTimeout, err:
            self.app.logger.warn(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            return HTTPRequestTimeout(request=req, headers=nexe_headers)
        except Exception:
            req.client_disconnect = True
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            return Response(status='499 Client Disconnect', headers=nexe_headers)
        if req.content_length and req.bytes_transferred < req.content_length:
            req.client_disconnect = True
            self.app.logger.warn(
                _('Client disconnected without sending enough data'))
            return Response(status='499 Client Disconnect', headers=nexe_headers)
        try:
            with Timeout(self.app.node_timeout):
                server_response = conn.getresponse()
        except (Exception, Timeout):
            self.exception_occurred(conn.node, _('Object'),
                _('Trying to get final status of POST to %s')
                % req.path_info)
            return Response(status='499 Client Disconnect', headers=nexe_headers)
        if server_response.status != 200:
            resp = Response(status='%d %s' % (server_response.status, server_response.reason),
                body=server_response.read())
            update_headers(resp, nexe_headers)
            return resp

        std_size = server_response.getheader('x-nexe-stdsize')
        if not std_size:
            std_size = [server_response.getheader('content-length')]
        else:
            std_size = [int(s) for s in std_size.split(' ')]
        req.bytes_transferred = 0

        client_response = ''
        dest_resp = ''
        seq_resp = SequentialResponseBody(server_response, std_size)
        def file_iter():
            try:
                while True:
                    with ChunkReadTimeout(self.app.node_timeout):
                        chunk = seq_resp.read(self.app.object_chunk_size)
                    if not chunk:
                        break
                    yield chunk
                    sleep()
                    #client_response.bytes_transferred += len(chunk)
            except GeneratorExit:
                client_response.client_disconnect = True
                self.app.logger.warn(_('Client disconnected on read'))
            except (Exception, Timeout):
                self.exception_occurred(conn.node, _('Object'),
                    _('Trying to read during QUERY of %s') % req.path_info)
                raise

        for std in stdlist:
            if std.path:
                dest_header = unquote(std.path)
                acct = req.path_info.split('/', 2)[1]
                dest_header = '/' + acct + dest_header
                dest_container_name, dest_obj_name = \
                    dest_header.split('/', 3)[2:]
                dest_req = Request.blank(dest_header)
                dest_req.environ['wsgi.input'] = seq_resp
                #dest_req.app_iter = self._make_app_iter(node, seq_resp, dest_req)
                dest_req.headers['Content-Length'] = seq_resp.get_content_length()
                dest_resp = \
                    ObjectController(self.app, acct, dest_container_name, dest_obj_name).PUT(dest_req)
                if dest_resp.status_int >= 300:
                    update_headers(dest_resp, nexe_headers)
                    return dest_resp
                update_headers(dest_resp, server_response.getheaders())
            else:
                if client_response:
                    client_response = Response(request=req,
                        conditional_response=True, body=server_response.read())
                else:
                    client_response = Response(request=req,
                        conditional_response=True, app_iter=file_iter())
                    client_response.content_length = seq_resp.get_content_length()
                update_headers(client_response, server_response.getheaders())
                update_headers(client_response, {'accept-ranges': 'bytes'})
                client_response.status = server_response.status
                if server_response.getheader('Content-Type'):
                    client_response.charset = None
                    client_response.content_type =\
                    server_response.getheader('Content-Type')
            seq_resp.next_response()
        if not client_response:
            if not dest_resp:
                raise Exception('Invalid response from obj server: %s %s'
                    % (server_response.status, server_response.read()))
            return dest_resp
        return client_response

    def zerovm_query(self, req):
        reader = req.environ['wsgi.input'].read
        upload_expiration = time.time() + self.app.max_upload_time
        etag = md5()
        cluster_config = ''
        req.bytes_transferred = 0
        for chunk in iter(lambda: reader(self.app.network_chunk_size), ''):
            req.bytes_transferred += len(chunk)
            if time.time() > upload_expiration:
                return HTTPRequestTimeout(request=req)
            etag.update(chunk)
            cluster_config += chunk

        if 'content-length' in req.headers and\
           int(req.headers['content-length']) != req.bytes_transferred:
            return Response(status='499 Client Disconnect')
        etag = etag.hexdigest()
        if 'etag' in req.headers and\
           req.headers['etag'].lower() != etag:
            return HTTPUnprocessableEntity(request=req)

        try:
            cluster_config = json.loads(cluster_config)
            nid = 1
            for node in cluster_config:
                node_name = node.get('name')
                if not node_name:
                    return HTTPBadRequest(request=req, body='Must specify node name')
                nexe = node.get('exec')
                if not nexe:
                    return HTTPBadRequest(request=req,
                        body='Must specify exec stanza for %s' % node_name)
                nexe_path = nexe.get('path')
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
                        access = device_map.get(device, -1)
                        if access < 0:
                            return HTTPBadRequest(request=req,
                                body='Unknown device %s in %s' % (device, node_name))
                        if not access & ACCESS_WRITABLE or access & ACCESS_CDR:
                            if len(read_list) > 0:
                                return HTTPBadRequest(request=req,
                                    body='More than one readable file in %s' % node_name)
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
                        if path and '*' in path:
                            read_group = 1
                            list = []
                            try:
                                container, object = split_path(path, 1, 2, True)
                            except ValueError:
                                return HTTPBadRequest(request=req,
                                    body='Invalid path %s in %s' % (path, node_name))
                            if '*' in container:
                                container = re.escape(container).replace('\\*', '.*')
                                mask = re.compile(container)
                                try:
                                    containers = self.list_account(req, self.account_name, mask)
                                except Exception:
                                    return HTTPBadRequest(request=req,
                                        body='Error querying object server for account %s'
                                        % self.account_name)
                                if object:
                                    if '*' in object:
                                        object = re.escape(object).replace('\\*', '.*')
                                    mask = re.compile(object)
                                else:
                                    mask = None
                                for c in containers:
                                    try:
                                        obj_list = self.list_container(req, self.account_name, c, mask)
                                    except Exception:
                                        return HTTPBadRequest(request=req,
                                            body='Error querying object server '
                                                 'for container %s' % c)
                                    for obj in obj_list:
                                        list.append('/' + c + '/' + obj)
                            else:
                                object = re.escape(object).replace('\\*', '.*')
                                mask = re.compile(object)
                                for obj in self.list_container(req, self.account_name, container, mask):
                                    list.append('/' + container + '/' + obj)
                            if not list:
                                return HTTPBadRequest(request=req, body='No objects found in path %s' % path)
                            for i in range(len(list)):
                                new_name = self.create_name(node_name, i+1)
                                new_path = list[i]
                                new_node = self.nodes.get(new_name)
                                if not new_node:
                                    new_node = ZvmNode(nid, new_name, nexe_path, node.get('args'), node.get('env'))
                                    nid += 1
                                    self.nodes[new_name] = new_node
                                new_node.add_channel(device, access, path=new_path)
                            node_count = len(list)
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
                        if path and '*' in path:
                            if read_group:
                                read_mask = read_list[0].get('path')
                                read_count = read_mask.count('*')
                                write_count = path.count('*')
                                if read_count != write_count:
                                    return HTTPBadRequest(request=req,
                                        body='Wildcards in input %s cannot be resolved into output %s'
                                        % (read_mask, path))
                                read_mask = re.escape(read_mask).replace('\\*', '(.*)')
                                read_mask = re.compile(read_mask)
                                for i in range(1, node_count + 1):
                                    new_name = self.create_name(node_name, i)
                                    new_path = path
                                    new_node = self.nodes.get(new_name)
                                    read_path = new_node.channels[0].path
                                    m = read_mask.match(read_path)
                                    for j in range(1, m.lastindex + 1):
                                        new_path = new_path.replace('*', m.group(j), 1)
                                    new_node.add_channel(device, access, path=new_path)
                            else:
                                for i in range(1, node_count + 1):
                                    new_name = self.create_name(node_name, i)
                                    new_path = path
                                    new_path = new_path.replace('*', new_name)
                                    new_node = self.nodes.get(new_name)
                                    if not new_node:
                                        new_node = ZvmNode(nid, new_name, nexe_path,
                                            node.get('args'), node.get('env'))
                                        nid += 1
                                        self.nodes[new_name] = new_node
                                    new_node.add_channel(device, access, path=new_path)
                        elif path:
                            if node_count > 1:
                                return HTTPBadRequest(request=req,
                                    body='Single path %s for multiple node '
                                         'definition: %s, please use wildcard'
                                    % (path, node_name))
                            new_node = self.nodes.get(node_name)
                            if not new_node:
                                new_node = ZvmNode(nid, node_name, nexe_path,
                                    node.get('args'), node.get('env'))
                                nid += 1
                                self.nodes[node_name] = new_node
                            new_node.add_channel(device, access, path=path)
                        else:
                            if 'stdout' not in device and 'stderr' not in device:
                                return HTTPBadRequest(request=req,
                                    body='Immediate response is not available for device %s' % device)
                            if node_count > 1:
                                for i in range(1, node_count + 1):
                                    new_name = self.create_name(node_name, i)
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
                        if not path:
                            return HTTPBadRequest(request=req, body='Path required for device %s' % device)
                        if node_count > 1:
                            for i in range(1, node_count + 1):
                                new_name = self.create_name(node_name, i)
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
            #for n in self.nodes.itervalues():
            #    print n.__dict__

        except Exception:
            print traceback.format_exc()
            return HTTPUnprocessableEntity(request=req)

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
            elif self.nodes.get(node_name + '-1'):
                j = 1
                connect_node = self.nodes.get(self.create_name(node_name, j))
                while connect_node:
                    try:
                        connect_node.add_connections(self.nodes, connect)
                    except Exception:
                        return HTTPBadRequest(request=req,
                            body='Invalid connect string for node %s' % connect_node.name)
                    j += 1
                    connect_node = self.nodes.get(self.create_name(node_name, j))
            else:
                return HTTPBadRequest(request=req,
                    body='Non existing node in connect string for node %s' % node_name)
        node_list = []
        for k in sorted(self.nodes.iterkeys()):
            node_list.append(self.nodes[k])
        #for n in node_list:
        #    print n.__dict__
        ns_server = NameService()
        if self.app.zerovm_ns_thrdpool.free() <= 0:
            return HTTPServiceUnavailable(body='Cluster slot not available',
                request=req, content_type='text/plain')
        ns_port = ns_server.start(self.app.zerovm_ns_thrdpool, len(self.nodes))

        pile = GreenPile(len(node_list))
        for node in node_list:
            pile.spawn(self.make_request, node, req, ns_port)
        for resp in pile:
            node_name = resp.headers.get('x-node-name')
            nexe_status = resp.headers.get('x-nexe-status')
            nexe_retcode = resp.headers.get('x-nexe-retcode')
            if nexe_retcode:
                nexe_retcode = int(nexe_retcode)
            nexe_etag = resp.headers.get('x-nexe-etag')
            node = self.nodes.get(node_name)
            if node:
                node.resp = ZvmResponse(node_name, resp.status, resp.body,
                    nexe_status, nexe_retcode, nexe_etag)
        resp_list = []
        for node in node_list:
            if getattr(node, 'resp', None):
                resp_list.append(node.resp.__dict__)
            else:
                resp_list.append(ZvmResponse(node.name, 503, '','','','').__dict__)
        final_response = Response(request=req, content_type='application/json')
        final_response.body = json.dumps(resp_list)
        ns_server.stop()
        return final_response

    def create_name(self, node_name, i):
        return node_name + '-' + str(i)


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def query_filter(app):
        return ProxyQueryMiddleware(app, conf)

    return query_filter
