# Copyright (c) 2010-2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NOTE: swift_conn
# You'll see swift_conn passed around a few places in this file. This is the
# source httplib connection of whatever it is attached to.
#   It is used when early termination of reading from the connection should
# happen, such as when a range request is satisfied but there's still more the
# source connection would like to send. To prevent having to read all the data
# that could be left, the source connection can be .close() and then reads
# commence to empty out any buffers.
#   These shenanigans are to ensure all related objects can be garbage
# collected. We've seen objects hang around forever otherwise.

from __future__ import with_statement
try:
    import simplejson as json
except ImportError:
    import json
import mimetypes
import os
import re
import time
import traceback
from ConfigParser import ConfigParser
from datetime import datetime
from urllib import unquote, quote
import uuid
import functools
from hashlib import md5
from random import shuffle

from eventlet import sleep, spawn_n, GreenPile, Timeout
from eventlet.queue import Queue, Empty, Full
from eventlet.timeout import Timeout
from webob.exc import HTTPAccepted, HTTPBadRequest, HTTPForbidden, \
    HTTPMethodNotAllowed, HTTPNotFound, HTTPPreconditionFailed, \
    HTTPRequestEntityTooLarge, HTTPRequestTimeout, HTTPServerError, \
    HTTPServiceUnavailable, status_map
from webob import Request, Response

from swift.common.ring import Ring
from swift.common.utils import cache_from_env, ContextPool, get_logger, \
    get_remote_client, normalize_timestamp, split_path, TRUE_VALUES, public
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_metadata, check_object_creation, \
    check_utf8, CONTAINER_LISTING_LIMIT, MAX_ACCOUNT_NAME_LENGTH, \
    MAX_CONTAINER_NAME_LENGTH, MAX_FILE_SIZE
from swift.common.exceptions import ChunkReadTimeout, \
    ChunkWriteTimeout, ConnectionTimeout, ListingIterNotFound, \
    ListingIterNotAuthorized, ListingIterError
from swift.common.http import is_informational, is_success, is_redirection, \
    is_client_error, is_server_error, HTTP_CONTINUE, HTTP_OK, HTTP_CREATED, \
    HTTP_ACCEPTED, HTTP_PARTIAL_CONTENT, HTTP_MULTIPLE_CHOICES, \
    HTTP_BAD_REQUEST, HTTP_NOT_FOUND, HTTP_REQUESTED_RANGE_NOT_SATISFIABLE, \
    HTTP_INTERNAL_SERVER_ERROR, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_INSUFFICIENT_STORAGE, HTTPClientDisconnect


def update_headers(response, headers):
    """
    Helper function to update headers in the response.

    :param response: webob.Response object
    :param headers: dictionary headers
    """
    if hasattr(headers, 'items'):
        headers = headers.items()
    for name, value in headers:
        if name == 'etag':
            response.headers[name] = value.replace('"', '')
        elif name not in ('date', 'content-length', 'content-type',
                          'connection', 'x-put-timestamp', 'x-delete-after'):
            response.headers[name] = value


def delay_denial(func):
    """
    Decorator to declare which methods should have any swift.authorize call
    delayed. This is so the method can load the Request object up with
    additional information that may be needed by the authorization system.

    :param func: function for which authorization will be delayed
    """
    func.delay_denial = True

    @functools.wraps(func)
    def wrapped(*a, **kw):
        return func(*a, **kw)
    return wrapped


def get_account_memcache_key(account):
    return 'account/%s' % account


def get_container_memcache_key(account, container):
    return 'container/%s/%s' % (account, container)


class SegmentedIterable(object):
    """
    Iterable that returns the object contents for a segmented object in Swift.

    If there's a failure that cuts the transfer short, the response's
    `status_int` will be updated (again, just for logging since the original
    status would have already been sent to the client).

    :param controller: The ObjectController instance to work with.
    :param container: The container the object segments are within.
    :param listing: The listing of object segments to iterate over; this may
                    be an iterator or list that returns dicts with 'name' and
                    'bytes' keys.
    :param response: The webob.Response this iterable is associated with, if
                     any (default: None)
    """

    def __init__(self, controller, container, listing, response=None):
        self.controller = controller
        self.container = container
        self.listing = iter(listing)
        self.segment = -1
        self.segment_dict = None
        self.segment_peek = None
        self.seek = 0
        self.segment_iter = None
        # See NOTE: swift_conn at top of file about this.
        self.segment_iter_swift_conn = None
        self.position = 0
        self.response = response
        if not self.response:
            self.response = Response()
        self.next_get_time = 0

    def _load_next_segment(self):
        """
        Loads the self.segment_iter with the next object segment's contents.

        :raises: StopIteration when there are no more object segments.
        """
        try:
            self.segment += 1
            self.segment_dict = self.segment_peek or self.listing.next()
            self.segment_peek = None
            partition, nodes = self.controller.app.object_ring.get_nodes(
                self.controller.account_name, self.container,
                self.segment_dict['name'])
            path = '/%s/%s/%s' % (self.controller.account_name, self.container,
                self.segment_dict['name'])
            req = Request.blank(path)
            if self.seek:
                req.range = 'bytes=%s-' % self.seek
                self.seek = 0
            if self.segment > self.controller.app.rate_limit_after_segment:
                sleep(max(self.next_get_time - time.time(), 0))
                self.next_get_time = time.time() + \
                    1.0 / self.controller.app.rate_limit_segments_per_sec
            shuffle(nodes)
            resp = self.controller.GETorHEAD_base(req, _('Object'), partition,
                self.controller.iter_nodes(partition, nodes,
                self.controller.app.object_ring), path,
                len(nodes))
            if not is_success(resp.status_int):
                raise Exception(_('Could not load object segment %(path)s:' \
                    ' %(status)s') % {'path': path, 'status': resp.status_int})
            self.segment_iter = resp.app_iter
            # See NOTE: swift_conn at top of file about this.
            self.segment_iter_swift_conn = getattr(resp, 'swift_conn', None)
        except StopIteration:
            raise
        except (Exception, Timeout), err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception(_('ERROR: While '
                    'processing manifest /%(acc)s/%(cont)s/%(obj)s'),
                    {'acc': self.controller.account_name,
                     'cont': self.controller.container_name,
                     'obj': self.controller.object_name})
                err.swift_logged = True
                self.response.status_int = HTTP_SERVICE_UNAVAILABLE
            raise

    def next(self):
        return iter(self).next()

    def __iter__(self):
        """ Standard iterator function that returns the object's contents. """
        try:
            while True:
                if not self.segment_iter:
                    self._load_next_segment()
                while True:
                    with ChunkReadTimeout(self.controller.app.node_timeout):
                        try:
                            chunk = self.segment_iter.next()
                            break
                        except StopIteration:
                            self._load_next_segment()
                self.position += len(chunk)
                yield chunk
        except StopIteration:
            raise
        except (Exception, Timeout), err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception(_('ERROR: While '
                    'processing manifest /%(acc)s/%(cont)s/%(obj)s'),
                    {'acc': self.controller.account_name,
                     'cont': self.controller.container_name,
                     'obj': self.controller.object_name})
                err.swift_logged = True
                self.response.status_int = HTTP_SERVICE_UNAVAILABLE
            raise

    def app_iter_range(self, start, stop):
        """
        Non-standard iterator function for use with Webob in serving Range
        requests more quickly. This will skip over segments and do a range
        request on the first segment to return data from, if needed.

        :param start: The first byte (zero-based) to return. None for 0.
        :param stop: The last byte (zero-based) to return. None for end.
        """
        try:
            if start:
                self.segment_peek = self.listing.next()
                while start >= self.position + self.segment_peek['bytes']:
                    self.segment += 1
                    self.position += self.segment_peek['bytes']
                    self.segment_peek = self.listing.next()
                self.seek = start - self.position
            else:
                start = 0
            if stop is not None:
                length = stop - start
            else:
                length = None
            for chunk in self:
                if length is not None:
                    length -= len(chunk)
                    if length < 0:
                        # Chop off the extra:
                        yield chunk[:length]
                        break
                yield chunk
            # See NOTE: swift_conn at top of file about this.
            if self.segment_iter_swift_conn:
                try:
                    self.segment_iter_swift_conn.close()
                except Exception:
                    pass
                self.segment_iter_swift_conn = None
            if self.segment_iter:
                try:
                    while self.segment_iter.next():
                        pass
                except Exception:
                    pass
                self.segment_iter = None
        except StopIteration:
            raise
        except (Exception, Timeout), err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception(_('ERROR: While '
                    'processing manifest /%(acc)s/%(cont)s/%(obj)s'),
                    {'acc': self.controller.account_name,
                     'cont': self.controller.container_name,
                     'obj': self.controller.object_name})
                err.swift_logged = True
                self.response.status_int = HTTP_SERVICE_UNAVAILABLE
            raise


class Controller(object):
    """Base WSGI controller class for the proxy"""
    server_type = _('Base')

    # Ensure these are all lowercase
    pass_through_headers = []

    def __init__(self, app):
        self.account_name = None
        self.app = app
        self.trans_id = '-'

    def transfer_headers(self, src_headers, dst_headers):
        x_remove = 'x-remove-%s-meta-' % self.server_type.lower()
        x_meta = 'x-%s-meta-' % self.server_type.lower()
        dst_headers.update((k.lower().replace('-remove', '', 1), '')
                           for k in src_headers
                           if k.lower().startswith(x_remove))
        dst_headers.update((k.lower(), v)
                           for k, v in src_headers.iteritems()
                           if k.lower() in self.pass_through_headers or
                              k.lower().startswith(x_meta))

    def error_increment(self, node):
        """
        Handles incrementing error counts when talking to nodes.

        :param node: dictionary of node to increment the error count for
        """
        node['errors'] = node.get('errors', 0) + 1
        node['last_error'] = time.time()

    def error_occurred(self, node, msg):
        """
        Handle logging, and handling of errors.

        :param node: dictionary of node to handle errors for
        :param msg: error message
        """
        self.error_increment(node)
        self.app.logger.error(_('%(msg)s %(ip)s:%(port)s'),
            {'msg': msg, 'ip': node['ip'], 'port': node['port']})

    def exception_occurred(self, node, typ, additional_info):
        """
        Handle logging of generic exceptions.

        :param node: dictionary of node to log the error for
        :param typ: server type
        :param additional_info: additional information to log
        """
        self.app.logger.exception(
            _('ERROR with %(type)s server %(ip)s:%(port)s/%(device)s re: '
              '%(info)s'),
            {'type': typ, 'ip': node['ip'], 'port': node['port'],
             'device': node['device'], 'info': additional_info})

    def error_limited(self, node):
        """
        Check if the node is currently error limited.

        :param node: dictionary of node to check
        :returns: True if error limited, False otherwise
        """
        now = time.time()
        if not 'errors' in node:
            return False
        if 'last_error' in node and node['last_error'] < \
                now - self.app.error_suppression_interval:
            del node['last_error']
            if 'errors' in node:
                del node['errors']
            return False
        limited = node['errors'] > self.app.error_suppression_limit
        if limited:
            self.app.logger.debug(
                _('Node error limited %(ip)s:%(port)s (%(device)s)'), node)
        return limited

    def error_limit(self, node):
        """
        Mark a node as error limited.

        :param node: dictionary of node to error limit
        """
        node['errors'] = self.app.error_suppression_limit + 1
        node['last_error'] = time.time()

    def account_info(self, account, autocreate=False):
        """
        Get account information, and also verify that the account exists.

        :param account: name of the account to get the info for
        :returns: tuple of (account partition, account nodes, container_count)
                  or (None, None, None) if it does not exist
        """
        partition, nodes = self.app.account_ring.get_nodes(account)
        # 0 = no responses, 200 = found, 404 = not found, -1 = mixed responses
        if self.app.memcache:
            cache_key = get_account_memcache_key(account)
            cache_value = self.app.memcache.get(cache_key)
            if not isinstance(cache_value, dict):
                result_code = cache_value
                container_count = 0
            else:
                result_code = cache_value['status']
                container_count = cache_value['container_count']
            if result_code == HTTP_OK:
                return partition, nodes, container_count
            elif result_code == HTTP_NOT_FOUND and not autocreate:
                return None, None, None
        result_code = 0
        container_count = 0
        attempts_left = len(nodes)
        path = '/%s' % account
        headers = {'x-trans-id': self.trans_id, 'Connection': 'close'}
        iternodes = self.iter_nodes(partition, nodes, self.app.account_ring)
        while attempts_left > 0:
            try:
                node = iternodes.next()
            except StopIteration:
                break
            attempts_left -= 1
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], partition, 'HEAD', path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    body = resp.read()
                    if is_success(resp.status):
                        result_code = HTTP_OK
                        container_count = int(
                            resp.getheader('x-account-container-count') or 0)
                        break
                    elif resp.status == HTTP_NOT_FOUND:
                        if result_code == 0:
                            result_code = HTTP_NOT_FOUND
                        elif result_code != HTTP_NOT_FOUND:
                            result_code = -1
                    elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                        self.error_limit(node)
                        continue
                    else:
                        result_code = -1
            except (Exception, Timeout):
                self.exception_occurred(node, _('Account'),
                    _('Trying to get account info for %s') % path)
        if result_code == HTTP_NOT_FOUND and autocreate:
            if len(account) > MAX_ACCOUNT_NAME_LENGTH:
                return None, None, None
            headers = {'X-Timestamp': normalize_timestamp(time.time()),
                       'X-Trans-Id': self.trans_id,
                       'Connection': 'close'}
            resp = self.make_requests(Request.blank('/v1' + path),
                self.app.account_ring, partition, 'PUT',
                path, [headers] * len(nodes))
            if not is_success(resp.status_int):
                raise Exception('Could not autocreate account %r' % path)
            result_code = HTTP_OK
        if self.app.memcache and result_code in (HTTP_OK, HTTP_NOT_FOUND):
            if result_code == HTTP_OK:
                cache_timeout = self.app.recheck_account_existence
            else:
                cache_timeout = self.app.recheck_account_existence * 0.1
            self.app.memcache.set(cache_key,
                {'status': result_code, 'container_count': container_count},
                timeout=cache_timeout)
        if result_code == HTTP_OK:
            return partition, nodes, container_count
        return None, None, None

    def container_info(self, account, container, account_autocreate=False):
        """
        Get container information and thusly verify container existance.
        This will also make a call to account_info to verify that the
        account exists.

        :param account: account name for the container
        :param container: container name to look up
        :returns: tuple of (container partition, container nodes, container
                  read acl, container write acl, container sync key) or (None,
                  None, None, None, None) if the container does not exist
        """
        partition, nodes = self.app.container_ring.get_nodes(
                account, container)
        path = '/%s/%s' % (account, container)
        if self.app.memcache:
            cache_key = get_container_memcache_key(account, container)
            cache_value = self.app.memcache.get(cache_key)
            if isinstance(cache_value, dict):
                status = cache_value['status']
                read_acl = cache_value['read_acl']
                write_acl = cache_value['write_acl']
                sync_key = cache_value.get('sync_key')
                versions = cache_value.get('versions')
                if status == HTTP_OK:
                    return partition, nodes, read_acl, write_acl, sync_key, \
                            versions
                elif status == HTTP_NOT_FOUND:
                    return None, None, None, None, None, None
        if not self.account_info(account, autocreate=account_autocreate)[1]:
            return None, None, None, None, None, None
        result_code = 0
        read_acl = None
        write_acl = None
        sync_key = None
        container_size = None
        versions = None
        attempts_left = len(nodes)
        headers = {'x-trans-id': self.trans_id, 'Connection': 'close'}
        iternodes = self.iter_nodes(partition, nodes, self.app.container_ring)
        while attempts_left > 0:
            try:
                node = iternodes.next()
            except StopIteration:
                break
            attempts_left -= 1
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], partition, 'HEAD', path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    body = resp.read()
                    if is_success(resp.status):
                        result_code = HTTP_OK
                        read_acl = resp.getheader('x-container-read')
                        write_acl = resp.getheader('x-container-write')
                        sync_key = resp.getheader('x-container-sync-key')
                        container_size = \
                            resp.getheader('X-Container-Object-Count')
                        versions = resp.getheader('x-versions-location')
                        break
                    elif resp.status == HTTP_NOT_FOUND:
                        if result_code == 0:
                            result_code = HTTP_NOT_FOUND
                        elif result_code != HTTP_NOT_FOUND:
                            result_code = -1
                    elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                        self.error_limit(node)
                        continue
                    else:
                        result_code = -1
            except (Exception, Timeout):
                self.exception_occurred(node, _('Container'),
                    _('Trying to get container info for %s') % path)
        if self.app.memcache and result_code in (HTTP_OK, HTTP_NOT_FOUND):
            if result_code == HTTP_OK:
                cache_timeout = self.app.recheck_container_existence
            else:
                cache_timeout = self.app.recheck_container_existence * 0.1
            self.app.memcache.set(cache_key,
                                  {'status': result_code,
                                   'read_acl': read_acl,
                                   'write_acl': write_acl,
                                   'sync_key': sync_key,
                                   'container_size': container_size,
                                   'versions': versions},
                                  timeout=cache_timeout)
        if result_code == HTTP_OK:
            return partition, nodes, read_acl, write_acl, sync_key, versions
        return None, None, None, None, None, None

    def iter_nodes(self, partition, nodes, ring):
        """
        Node iterator that will first iterate over the normal nodes for a
        partition and then the handoff partitions for the node.

        :param partition: partition to iterate nodes for
        :param nodes: list of node dicts from the ring
        :param ring: ring to get handoff nodes from
        """
        for node in nodes:
            if not self.error_limited(node):
                yield node
        handoffs = 0
        for node in ring.get_more_nodes(partition):
            if not self.error_limited(node):
                handoffs += 1
                if self.app.log_handoffs:
                    self.app.logger.increment('handoff_count')
                    self.app.logger.warning(
                        'Handoff requested (%d)' % handoffs)
                    if handoffs == len(nodes):
                        self.app.logger.increment('handoff_all_count')
                yield node

    def _make_request(self, nodes, part, method, path, headers, query,
                      logger_thread_locals):
        self.app.logger.thread_locals = logger_thread_locals
        for node in nodes:
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], part, method, path,
                            headers=headers, query_string=query)
                    conn.node = node
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    if not is_informational(resp.status) and \
                       not is_server_error(resp.status):
                        return resp.status, resp.reason, resp.read()
                    elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                        self.error_limit(node)
            except (Exception, Timeout):
                self.exception_occurred(node, self.server_type,
                    _('Trying to %(method)s %(path)s') %
                    {'method': method, 'path': path})

    def make_requests(self, req, ring, part, method, path, headers,
                    query_string=''):
        """
        Sends an HTTP request to multiple nodes and aggregates the results.
        It attempts the primary nodes concurrently, then iterates over the
        handoff nodes as needed.

        :param headers: a list of dicts, where each dict represents one
                        backend request that should be made.
        :returns: a webob Response object
        """
        start_nodes = ring.get_part_nodes(part)
        nodes = self.iter_nodes(part, start_nodes, ring)
        pile = GreenPile(len(start_nodes))
        for head in headers:
            pile.spawn(self._make_request, nodes, part, method, path,
                       head, query_string, self.app.logger.thread_locals)
        response = [resp for resp in pile if resp]
        while len(response) < len(start_nodes):
            response.append((HTTP_SERVICE_UNAVAILABLE, '', ''))
        statuses, reasons, bodies = zip(*response)
        return self.best_response(req, statuses, reasons, bodies,
                  '%s %s' % (self.server_type, req.method))

    def best_response(self, req, statuses, reasons, bodies, server_type,
                      etag=None):
        """
        Given a list of responses from several servers, choose the best to
        return to the API.

        :param req: webob.Request object
        :param statuses: list of statuses returned
        :param reasons: list of reasons for each status
        :param bodies: bodies of each response
        :param server_type: type of server the responses came from
        :param etag: etag
        :returns: webob.Response object with the correct status, body, etc. set
        """
        resp = Response(request=req)
        if len(statuses):
            for hundred in (HTTP_OK, HTTP_MULTIPLE_CHOICES, HTTP_BAD_REQUEST):
                hstatuses = \
                    [s for s in statuses if hundred <= s < hundred + 100]
                if len(hstatuses) > len(statuses) / 2:
                    status = max(hstatuses)
                    status_index = statuses.index(status)
                    resp.status = '%s %s' % (status, reasons[status_index])
                    resp.body = bodies[status_index]
                    resp.content_type = 'text/html'
                    if etag:
                        resp.headers['etag'] = etag.strip('"')
                    return resp
        self.app.logger.error(_('%(type)s returning 503 for %(statuses)s'),
                              {'type': server_type, 'statuses': statuses})
        resp.status = '503 Internal Server Error'
        return resp

    @public
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req, stats_type='GET')

    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req, stats_type='HEAD')

    def _make_app_iter_reader(self, node, source, queue, logger_thread_locals):
        """
        Reads from the source and places data in the queue. It expects
        something else be reading from the queue and, if nothing does within
        self.app.client_timeout seconds, the process will be aborted.

        :param node: The node dict that the source is connected to, for
                     logging/error-limiting purposes.
        :param source: The httplib.Response object to read from.
        :param queue: The eventlet.queue.Queue to place read source data into.
        :param logger_thread_locals: The thread local values to be set on the
                                     self.app.logger to retain transaction
                                     logging information.
        """
        self.app.logger.thread_locals = logger_thread_locals
        try:
            try:
                while True:
                    with ChunkReadTimeout(self.app.node_timeout):
                        chunk = source.read(self.app.object_chunk_size)
                    if not chunk:
                        break
                    queue.put(chunk, timeout=self.app.client_timeout)
            except Full:
                self.app.logger.warn(
                    _('Client did not read from queue within %ss') %
                    self.app.client_timeout)
                self.app.logger.increment('client_timeouts')
            except (Exception, Timeout):
                self.exception_occurred(node, _('Object'),
                   _('Trying to read during GET'))
        finally:
            # Ensure the queue getter gets an empty-string-terminator.
            queue.resize(2)
            queue.put('')
            # Close-out the connection as best as possible.
            if getattr(source, 'swift_conn', None):
                try:
                    source.swift_conn.close()
                except Exception:
                    pass
                source.swift_conn = None
                try:
                    while source.read(self.app.object_chunk_size):
                        pass
                except Exception:
                    pass
                try:
                    source.close()
                except Exception:
                    pass

    def _make_app_iter(self, node, source, response):
        """
        Returns an iterator over the contents of the source (via its read
        func).  There is also quite a bit of cleanup to ensure garbage
        collection works and the underlying socket of the source is closed.

        :param response: The webob.Response object this iterator should be
                         assigned to via response.app_iter.
        :param source: The httplib.Response object this iterator should read
                       from.
        :param node: The node the source is reading from, for logging purposes.
        """
        try:
            try:
                # Spawn reader to read from the source and place in the queue.
                # We then drop any reference to the source or node, for garbage
                # collection purposes.
                queue = Queue(1)
                spawn_n(self._make_app_iter_reader, node, source, queue,
                        self.app.logger.thread_locals)
                source = node = None
                while True:
                    chunk = queue.get(timeout=self.app.node_timeout)
                    if not chunk:
                        break
                    yield chunk
            except Empty:
                raise ChunkReadTimeout()
            except (GeneratorExit, Timeout):
                self.app.logger.warn(_('Client disconnected on read'))
            except Exception:
                self.app.logger.exception(_('Trying to send to client'))
                raise
        finally:
            response.app_iter = None

    def GETorHEAD_base(self, req, server_type, partition, nodes, path,
                       attempts):
        """
        Base handler for HTTP GET or HEAD requests.

        :param req: webob.Request object
        :param server_type: server type
        :param partition: partition
        :param nodes: nodes
        :param path: path for the request
        :param attempts: number of attempts to try
        :returns: webob.Response object
        """
        statuses = []
        reasons = []
        bodies = []
        source = None
        sources = []
        try:
            _junk = int(req.headers.get('x-revision', '-1'))
        except ValueError:
            return HTTPBadRequest(request=req,
                content_type='text/plain',
                body='Non-integer X-Revision')
        newest = req.headers.get('x-newest', 'f').lower() in TRUE_VALUES
        nodes = iter(nodes)
        while len(statuses) < attempts:
            try:
                node = nodes.next()
            except StopIteration:
                break
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    headers = dict(req.headers)
                    headers['Connection'] = 'close'
                    conn = http_connect(node['ip'], node['port'],
                        node['device'], partition, req.method, path,
                        headers=headers,
                        query_string=req.query_string)
                with Timeout(self.app.node_timeout):
                    possible_source = conn.getresponse()
                    # See NOTE: swift_conn at top of file about this.
                    possible_source.swift_conn = conn
            except (Exception, Timeout):
                self.exception_occurred(node, server_type,
                    _('Trying to %(method)s %(path)s') %
                    {'method': req.method, 'path': req.path})
                continue
            if possible_source.status == HTTP_INSUFFICIENT_STORAGE:
                self.error_limit(node)
                continue
            if is_success(possible_source.status) or \
               is_redirection(possible_source.status):
                # 404 if we know we don't have a synced copy
                if not float(possible_source.getheader('X-PUT-Timestamp', 1)):
                    statuses.append(HTTP_NOT_FOUND)
                    reasons.append('')
                    bodies.append('')
                    possible_source.read()
                    continue
                if newest:
                    if sources:
                        ts = float(source.getheader('x-put-timestamp') or
                                   source.getheader('x-timestamp') or 0)
                        pts = float(
                            possible_source.getheader('x-put-timestamp') or
                            possible_source.getheader('x-timestamp') or 0)
                        if pts > ts:
                            sources.insert(0, possible_source)
                        else:
                            sources.append(possible_source)
                    else:
                        sources.insert(0, possible_source)
                    source = sources[0]
                    statuses.append(source.status)
                    reasons.append(source.reason)
                    bodies.append('')
                    continue
                else:
                    source = possible_source
                    break
            statuses.append(possible_source.status)
            reasons.append(possible_source.reason)
            bodies.append(possible_source.read())
            if is_server_error(possible_source.status):
                self.error_occurred(node, _('ERROR %(status)d %(body)s ' \
                    'From %(type)s Server') %
                    {'status': possible_source.status,
                    'body': bodies[-1][:1024], 'type': server_type})
        if source:
            if newest:
                # we need to close all the hanging swift_conns
                sources.pop(0)
                for src in sources:
                    try:
                        src.swift_conn.close()
                    except Exception:
                        pass
                    src.swift_conn = None
                    try:
                        while src.read(self.app.object_chunk_size):
                            pass
                    except Exception:
                        pass
                    try:
                        src.close()
                    except Exception:
                        pass
            if req.method == 'GET' and \
                source.status in (HTTP_OK, HTTP_PARTIAL_CONTENT):
                res = Response(request=req, conditional_response=True)
                res.app_iter = self._make_app_iter(node, source, res)
                # See NOTE: swift_conn at top of file about this.
                res.swift_conn = source.swift_conn
                update_headers(res, source.getheaders())
                # Used by container sync feature
                if res.environ is None:
                    res.environ = dict()
                res.environ['swift_x_timestamp'] = \
                    source.getheader('x-timestamp')
                update_headers(res, {'accept-ranges': 'bytes'})
                res.status = source.status
                res.content_length = source.getheader('Content-Length')
                if source.getheader('Content-Type'):
                    res.charset = None
                    res.content_type = source.getheader('Content-Type')
                return res
            elif is_success(source.status) or is_redirection(source.status):
                res = status_map[source.status](request=req)
                update_headers(res, source.getheaders())
                # Used by container sync feature
                if res.environ is None:
                    res.environ = dict()
                res.environ['swift_x_timestamp'] = \
                    source.getheader('x-timestamp')
                update_headers(res, {'accept-ranges': 'bytes'})
                res.content_length = source.getheader('Content-Length')
                if source.getheader('Content-Type'):
                    res.charset = None
                    res.content_type = source.getheader('Content-Type')
                return res
        return self.best_response(req, statuses, reasons, bodies,
                                  '%s %s' % (server_type, req.method))


class ObjectController(Controller):
    """WSGI controller for object requests."""
    server_type = _('Object')

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)

    def _listing_iter(self, lcontainer, lprefix, env):
        lpartition, lnodes = self.app.container_ring.get_nodes(
            self.account_name, lcontainer)
        marker = ''
        while True:
            lreq = Request.blank('i will be overridden by env', environ=env)
            # Don't quote PATH_INFO, by WSGI spec
            lreq.environ['PATH_INFO'] = \
                '/%s/%s' % (self.account_name, lcontainer)
            lreq.environ['REQUEST_METHOD'] = 'GET'
            lreq.environ['QUERY_STRING'] = \
                'format=json&prefix=%s&marker=%s' % (quote(lprefix),
                                                     quote(marker))
            shuffle(lnodes)
            lresp = self.GETorHEAD_base(lreq, _('Container'),
                lpartition, lnodes, lreq.path_info,
                len(lnodes))
            if 'swift.authorize' in env:
                lreq.acl = lresp.headers.get('x-container-read')
                aresp = env['swift.authorize'](lreq)
                if aresp:
                    raise ListingIterNotAuthorized(aresp)
            if lresp.status_int == HTTP_NOT_FOUND:
                raise ListingIterNotFound()
            elif not is_success(lresp.status_int):
                raise ListingIterError()
            if not lresp.body:
                break
            sublisting = json.loads(lresp.body)
            if not sublisting:
                break
            marker = sublisting[-1]['name']
            for obj in sublisting:
                yield obj

    def GETorHEAD(self, req, stats_type):
        """Handle HTTP GET or HEAD requests."""
        start_time = time.time()
        _junk, _junk, req.acl, _junk, _junk, object_versions = \
            self.container_info(self.account_name, self.container_name)
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                self.app.logger.increment('auth_short_circuits')
                return aresp
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        shuffle(nodes)
        resp = self.GETorHEAD_base(req, _('Object'), partition,
                self.iter_nodes(partition, nodes, self.app.object_ring),
                req.path_info, len(nodes))
        # Whether we get a 416 Requested Range Not Satisfiable or not,
        # we should request a manifest because size of manifest file
        # can be not 0. After checking a manifest, redo the range request
        # on the whole object.
        if req.range:
            req_range = req.range
            req.range = None
            resp2 = self.GETorHEAD_base(req, _('Object'), partition,
                                        self.iter_nodes(partition,
                                                        nodes,
                                                        self.app.object_ring),
                                        req.path_info, len(nodes))
            if 'x-object-manifest' not in resp2.headers:
                self.app.logger.timing_since(
                    '%s.timing' % (stats_type,), start_time)
                return resp
            resp = resp2
            req.range = str(req_range)

        if 'x-object-manifest' in resp.headers:
            lcontainer, lprefix = \
                resp.headers['x-object-manifest'].split('/', 1)
            lcontainer = unquote(lcontainer)
            lprefix = unquote(lprefix)
            try:
                listing = list(self._listing_iter(lcontainer, lprefix,
                                req.environ))
            except ListingIterNotFound:
                self.app.logger.timing_since(
                    '%s.timing' % (stats_type,), start_time)
                return HTTPNotFound(request=req)
            except ListingIterNotAuthorized, err:
                self.app.logger.increment('auth_short_circuits')
                return err.aresp
            except ListingIterError:
                self.app.logger.increment('errors')
                return HTTPServerError(request=req)

            if len(listing) > CONTAINER_LISTING_LIMIT:
                resp = Response(headers=resp.headers, request=req,
                                conditional_response=True)
                if req.method == 'HEAD':
                    # These shenanigans are because webob translates the HEAD
                    # request into a webob EmptyResponse for the body, which
                    # has a len, which eventlet translates as needing a
                    # content-length header added. So we call the original
                    # webob resp for the headers but return an empty iterator
                    # for the body.

                    def head_response(environ, start_response):
                        resp(environ, start_response)
                        return iter([])

                    head_response.status_int = resp.status_int
                    self.app.logger.timing_since(
                        '%s.timing' % (stats_type,), start_time)
                    return head_response
                else:
                    resp.app_iter = SegmentedIterable(self, lcontainer,
                        self._listing_iter(lcontainer, lprefix, req.environ),
                        resp)

            else:
                # For objects with a reasonable number of segments, we'll serve
                # them with a set content-length and computed etag.
                if listing:
                    content_length = sum(o['bytes'] for o in listing)
                    last_modified = max(o['last_modified'] for o in listing)
                    last_modified = datetime(*map(int, re.split('[^\d]',
                        last_modified)[:-1]))
                    etag = md5(
                        ''.join(o['hash'] for o in listing)).hexdigest()
                else:
                    content_length = 0
                    last_modified = resp.last_modified
                    etag = md5().hexdigest()
                resp = Response(headers=resp.headers, request=req,
                                conditional_response=True)
                resp.app_iter = SegmentedIterable(self, lcontainer, listing,
                                                  resp)
                resp.content_length = content_length
                resp.last_modified = last_modified
                resp.etag = etag
            resp.headers['accept-ranges'] = 'bytes'

        self.app.logger.timing_since('%s.timing' % (stats_type,), start_time)
        return resp

    @public
    @delay_denial
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req, stats_type='GET')

    @public
    @delay_denial
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req, stats_type='HEAD')

    @public
    @delay_denial
    def POST(self, req):
        """HTTP POST request handler."""
        start_time = time.time()
        if 'x-delete-after' in req.headers:
            try:
                x_delete_after = int(req.headers['x-delete-after'])
            except ValueError:
                self.app.logger.increment('errors')
                return HTTPBadRequest(request=req,
                                      content_type='text/plain',
                                      body='Non-integer X-Delete-After')
            req.headers['x-delete-at'] = '%d' % (time.time() + x_delete_after)
        if 'x-append-to' in req.headers:
            try:
                _junk = int(req.headers['x-append-to'])
            except ValueError:
                self.app.logger.increment('errors')
                return HTTPBadRequest(request=req,
                    content_type='text/plain',
                    body='Non-integer X-Append-To')
            req.method = 'POST'
            req.path_info = '/%s/%s/%s' % (self.account_name,
                                           self.container_name, self.object_name)
            resp = self.PUT(req, start_time=start_time, stats_type='POST', append=True)
            if resp.status_int != HTTP_CREATED:
                return resp
            return HTTPAccepted(request=req)
        elif self.app.object_post_as_copy:
            req.method = 'PUT'
            req.path_info = '/%s/%s/%s' % (self.account_name,
                self.container_name, self.object_name)
            req.headers['Content-Length'] = 0
            req.headers['X-Copy-From'] = quote('/%s/%s' % (self.container_name,
                self.object_name))
            req.headers['X-Fresh-Metadata'] = 'true'
            req.environ['swift_versioned_copy'] = True
            resp = self.PUT(req, start_time=start_time, stats_type='POST')
            # Older editions returned 202 Accepted on object POSTs, so we'll
            # convert any 201 Created responses to that for compatibility with
            # picky clients.
            if resp.status_int != HTTP_CREATED:
                return resp
            return HTTPAccepted(request=req)
        else:
            error_response = check_metadata(req, 'object')
            if error_response:
                self.app.logger.increment('errors')
                return error_response
            container_partition, containers, _junk, req.acl, _junk, _junk = \
                self.container_info(self.account_name, self.container_name,
                    account_autocreate=self.app.account_autocreate)
            if 'swift.authorize' in req.environ:
                aresp = req.environ['swift.authorize'](req)
                if aresp:
                    self.app.logger.increment('auth_short_circuits')
                    return aresp
            if not containers:
                self.app.logger.timing_since('POST.timing', start_time)
                return HTTPNotFound(request=req)
            if 'x-delete-at' in req.headers:
                try:
                    x_delete_at = int(req.headers['x-delete-at'])
                    if x_delete_at < time.time():
                        self.app.logger.increment('errors')
                        return HTTPBadRequest(body='X-Delete-At in past',
                            request=req, content_type='text/plain')
                except ValueError:
                    self.app.logger.increment('errors')
                    return HTTPBadRequest(request=req,
                                          content_type='text/plain',
                                          body='Non-integer X-Delete-At')
                delete_at_container = str(x_delete_at /
                    self.app.expiring_objects_container_divisor *
                    self.app.expiring_objects_container_divisor)
                delete_at_part, delete_at_nodes = \
                    self.app.container_ring.get_nodes(
                        self.app.expiring_objects_account, delete_at_container)
            else:
                delete_at_part = delete_at_nodes = None
            partition, nodes = self.app.object_ring.get_nodes(
                self.account_name, self.container_name, self.object_name)
            req.headers['X-Timestamp'] = normalize_timestamp(time.time())
            headers = []
            for container in containers:
                nheaders = dict(req.headers.iteritems())
                nheaders['Connection'] = 'close'
                nheaders['X-Container-Host'] = '%(ip)s:%(port)s' % container
                nheaders['X-Container-Partition'] = container_partition
                nheaders['X-Container-Device'] = container['device']
                if delete_at_nodes:
                    node = delete_at_nodes.pop(0)
                    nheaders['X-Delete-At-Host'] = '%(ip)s:%(port)s' % node
                    nheaders['X-Delete-At-Partition'] = delete_at_part
                    nheaders['X-Delete-At-Device'] = node['device']
                headers.append(nheaders)
            resp = self.make_requests(req, self.app.object_ring, partition,
                                      'POST', req.path_info, headers)
            self.app.logger.timing_since('POST.timing', start_time)
            return resp

    def _send_file(self, conn, path):
        """Method for a file PUT coro"""
        while True:
            chunk = conn.queue.get()
            if not conn.failed:
                try:
                    with ChunkWriteTimeout(self.app.node_timeout):
                        conn.send(chunk)
                except (Exception, ChunkWriteTimeout):
                    conn.failed = True
                    self.exception_occurred(conn.node, _('Object'),
                        _('Trying to write to %s') % path)
            conn.queue.task_done()

    def _connect_put_node(self, nodes, part, path, headers,
                          logger_thread_locals, append=False):
        """Method for a file PUT connect"""
        self.app.logger.thread_locals = logger_thread_locals
        for node in nodes:
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], part,
                        'POST' if append else 'PUT', path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getexpect()
                if resp.status == HTTP_CONTINUE:
                    conn.node = node
                    return conn
                elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                    self.error_limit(node)
            except:
                self.exception_occurred(node, _('Object'),
                    _('Expect: 100-continue on %s') % path)

    @public
    @delay_denial
    def PUT(self, req, start_time=None, stats_type='PUT', append=False):
        """HTTP PUT request handler."""
        if not start_time:
            start_time = time.time()
        (container_partition, containers, _junk, req.acl,
         req.environ['swift_sync_key'], object_versions) = \
            self.container_info(self.account_name, self.container_name,
                account_autocreate=self.app.account_autocreate)
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                self.app.logger.increment('auth_short_circuits')
                return aresp
        if not containers:
            self.app.logger.timing_since(
                '%s.timing' % (stats_type,), start_time)
            return HTTPNotFound(request=req)
        if 'x-delete-after' in req.headers:
            try:
                x_delete_after = int(req.headers['x-delete-after'])
            except ValueError:
                    self.app.logger.increment('errors')
                    return HTTPBadRequest(request=req,
                                          content_type='text/plain',
                                          body='Non-integer X-Delete-After')
            req.headers['x-delete-at'] = '%d' % (time.time() + x_delete_after)
        if 'x-delete-at' in req.headers:
            try:
                x_delete_at = int(req.headers['x-delete-at'])
                if x_delete_at < time.time():
                    self.app.logger.increment('errors')
                    return HTTPBadRequest(body='X-Delete-At in past',
                        request=req, content_type='text/plain')
            except ValueError:
                self.app.logger.increment('errors')
                return HTTPBadRequest(request=req, content_type='text/plain',
                                      body='Non-integer X-Delete-At')
            delete_at_container = str(x_delete_at /
                self.app.expiring_objects_container_divisor *
                self.app.expiring_objects_container_divisor)
            delete_at_part, delete_at_nodes = \
                self.app.container_ring.get_nodes(
                    self.app.expiring_objects_account, delete_at_container)
        else:
            delete_at_part = delete_at_nodes = None
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        # do a HEAD request for container sync and checking object versions
        if 'x-timestamp' in req.headers or (object_versions and not
                                    req.environ.get('swift_versioned_copy')):
            hreq = Request.blank(req.path_info, headers={'X-Newest': 'True'},
                                 environ={'REQUEST_METHOD': 'HEAD'})
            hresp = self.GETorHEAD_base(hreq, _('Object'), partition, nodes,
                hreq.path_info, len(nodes))
        # Used by container sync feature
        if 'x-timestamp' in req.headers:
            try:
                req.headers['X-Timestamp'] = \
                    normalize_timestamp(float(req.headers['x-timestamp']))
                if hresp.environ and 'swift_x_timestamp' in hresp.environ and \
                    float(hresp.environ['swift_x_timestamp']) >= \
                        float(req.headers['x-timestamp']):
                    self.app.logger.timing_since(
                        '%.timing' % (stats_type,), start_time)
                    return HTTPAccepted(request=req)
            except ValueError:
                self.app.logger.increment('errors')
                return HTTPBadRequest(request=req, content_type='text/plain',
                    body='X-Timestamp should be a UNIX timestamp float value; '
                         'was %r' % req.headers['x-timestamp'])
        else:
            req.headers['X-Timestamp'] = normalize_timestamp(time.time())
        # Sometimes the 'content-type' header exists, but is set to None.
        content_type_manually_set = True
        if not req.headers.get('content-type'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            req.headers['Content-Type'] = guessed_type or \
                                                'application/octet-stream'
            content_type_manually_set = False
        error_response = check_object_creation(req, self.object_name)
        if error_response:
            self.app.logger.increment('errors')
            return error_response
        if object_versions and not req.environ.get('swift_versioned_copy'):
            is_manifest = 'x-object-manifest' in req.headers or \
                          'x-object-manifest' in hresp.headers
            if hresp.status_int != HTTP_NOT_FOUND and not is_manifest:
                # This is a version manifest and needs to be handled
                # differently. First copy the existing data to a new object,
                # then write the data from this request to the version manifest
                # object.
                lcontainer = object_versions.split('/')[0]
                prefix_len = '%03x' % len(self.object_name)
                lprefix = prefix_len + self.object_name + '/'
                ts_source = hresp.environ.get('swift_x_timestamp')
                if ts_source is None:
                    ts_source = time.mktime(time.strptime(
                                            hresp.headers['last-modified'],
                                            '%a, %d %b %Y %H:%M:%S GMT'))
                new_ts = normalize_timestamp(ts_source)
                vers_obj_name = lprefix + new_ts
                copy_headers = {
                    'Destination': '%s/%s' % (lcontainer, vers_obj_name)}
                copy_environ = {'REQUEST_METHOD': 'COPY',
                                'swift_versioned_copy': True
                               }
                copy_req = Request.blank(req.path_info, headers=copy_headers,
                                environ=copy_environ)
                copy_resp = self.COPY(copy_req)
                if is_client_error(copy_resp.status_int):
                    # missing container or bad permissions
                    return HTTPPreconditionFailed(request=req)
                elif not is_success(copy_resp.status_int):
                    # could not copy the data, bail
                    return HTTPServiceUnavailable(request=req)

        reader = req.environ['wsgi.input'].read
        data_source = iter(lambda: reader(self.app.client_chunk_size), '')
        source_header = req.headers.get('X-Copy-From')
        source_resp = None
        if source_header:
            source_header = unquote(source_header)
            acct = req.path_info.split('/', 2)[1]
            if isinstance(acct, unicode):
                acct = acct.encode('utf-8')
            if not source_header.startswith('/'):
                source_header = '/' + source_header
            source_header = '/' + acct + source_header
            try:
                src_container_name, src_obj_name = \
                    source_header.split('/', 3)[2:]
            except ValueError:
                self.app.logger.increment('errors')
                return HTTPPreconditionFailed(request=req,
                    body='X-Copy-From header must be of the form'
                    '<container name>/<object name>')
            source_req = req.copy_get()
            source_req.path_info = source_header
            source_req.headers['X-Newest'] = 'true'
            orig_obj_name = self.object_name
            orig_container_name = self.container_name
            self.object_name = src_obj_name
            self.container_name = src_container_name
            source_resp = self.GET(source_req)
            if source_resp.status_int >= HTTP_MULTIPLE_CHOICES:
                self.app.logger.timing_since(
                    '%s.timing' % (stats_type,), start_time)
                return source_resp
            self.object_name = orig_obj_name
            self.container_name = orig_container_name
            new_req = Request.blank(req.path_info,
                        environ=req.environ, headers=req.headers)
            data_source = source_resp.app_iter
            new_req.content_length = source_resp.content_length
            if new_req.content_length is None:
                # This indicates a transfer-encoding: chunked source object,
                # which currently only happens because there are more than
                # CONTAINER_LISTING_LIMIT segments in a segmented object. In
                # this case, we're going to refuse to do the server-side copy.
                self.app.logger.increment('errors')
                return HTTPRequestEntityTooLarge(request=req)
            new_req.etag = source_resp.etag
            # we no longer need the X-Copy-From header
            del new_req.headers['X-Copy-From']
            if not content_type_manually_set:
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
        node_iter = self.iter_nodes(partition, nodes, self.app.object_ring)
        pile = GreenPile(len(nodes))
        for container in containers:
            nheaders = dict(req.headers.iteritems())
            nheaders['Connection'] = 'close'
            nheaders['X-Container-Host'] = '%(ip)s:%(port)s' % container
            nheaders['X-Container-Partition'] = container_partition
            nheaders['X-Container-Device'] = container['device']
            nheaders['Expect'] = '100-continue'
            if delete_at_nodes:
                node = delete_at_nodes.pop(0)
                nheaders['X-Delete-At-Host'] = '%(ip)s:%(port)s' % node
                nheaders['X-Delete-At-Partition'] = delete_at_part
                nheaders['X-Delete-At-Device'] = node['device']
            pile.spawn(self._connect_put_node, node_iter, partition,
                       req.path_info, nheaders, self.app.logger.thread_locals, append)
        conns = [conn for conn in pile if conn]
        if len(conns) <= len(nodes) / 2:
            self.app.logger.error(
                _('Object %(stats)s returning 503, %(conns)s/%(nodes)s '
                'required connections'),
                {'conns': len(conns),
                 'nodes': len(nodes) // 2 + 1,
                 'stats': stats_type})
            self.app.logger.increment('errors')
            return HTTPServiceUnavailable(request=req)
        chunked = req.headers.get('transfer-encoding')
        bytes_transferred = 0
        try:
            with ContextPool(len(nodes)) as pool:
                for conn in conns:
                    conn.failed = False
                    conn.queue = Queue(self.app.put_queue_depth)
                    pool.spawn(self._send_file, conn, req.path)
                while True:
                    with ChunkReadTimeout(self.app.client_timeout):
                        try:
                            chunk = next(data_source)
                        except StopIteration:
                            if chunked:
                                [conn.queue.put('0\r\n\r\n') for conn in conns]
                            break
                    bytes_transferred += len(chunk)
                    if bytes_transferred > MAX_FILE_SIZE:
                        self.app.logger.increment('errors')
                        return HTTPRequestEntityTooLarge(request=req)
                    for conn in list(conns):
                        if not conn.failed:
                            conn.queue.put('%x\r\n%s\r\n' % (len(chunk), chunk)
                                            if chunked else chunk)
                        else:
                            conns.remove(conn)
                    if len(conns) <= len(nodes) / 2:
                        self.app.logger.error(_('Object %(stats)s exceptions during'
                            ' send, %(conns)s/%(nodes)s required connections'),
                            {'conns': len(conns),
                             'nodes': len(nodes) / 2 + 1,
                             'stats': stats_type})
                        self.app.logger.increment('errors')
                        return HTTPServiceUnavailable(request=req)
                for conn in conns:
                    if conn.queue.unfinished_tasks:
                        conn.queue.join()
            conns = [conn for conn in conns if not conn.failed]
        except ChunkReadTimeout, err:
            self.app.logger.warn(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            self.app.logger.increment('client_timeouts')
            return HTTPRequestTimeout(request=req)
        except (Exception, Timeout):
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            self.app.logger.increment('client_disconnects')
            self.app.logger.timing_since(
                '%s.timing' % (stats_type,), start_time)
            return HTTPClientDisconnect(request=req)
        if req.content_length and bytes_transferred < req.content_length:
            req.client_disconnect = True
            self.app.logger.warn(
                _('Client disconnected without sending enough data'))
            self.app.logger.increment('client_disconnects')
            self.app.logger.timing_since(
                '%s.timing' % (stats_type,), start_time)
            return HTTPClientDisconnect(request=req)
        statuses = []
        reasons = []
        bodies = []
        etags = set()
        for conn in conns:
            try:
                with Timeout(self.app.node_timeout):
                    response = conn.getresponse()
                    statuses.append(response.status)
                    reasons.append(response.reason)
                    bodies.append(response.read())
                    if response.status >= HTTP_INTERNAL_SERVER_ERROR:
                        self.error_occurred(conn.node,
                            _('ERROR %(status)d %(body)s From Object Server ' \
                            're: %(path)s') % {'status': response.status,
                            'body': bodies[-1][:1024], 'path': req.path})
                    elif is_success(response.status):
                        etags.add(response.getheader('etag').strip('"'))
            except (Exception, Timeout):
                self.exception_occurred(conn.node, _('Object'),
                    _('Trying to get final status of %s to %s') % (stats_type, req.path))
        if len(etags) > 1:
            self.app.logger.error(
                _('Object servers returned %s mismatched etags'), len(etags))
            self.app.logger.increment('errors')
            return HTTPServerError(request=req)
        etag = len(etags) and etags.pop() or None
        while len(statuses) < len(nodes):
            statuses.append(HTTP_SERVICE_UNAVAILABLE)
            reasons.append('')
            bodies.append('')
        resp = self.best_response(req, statuses, reasons, bodies,
                    _('Object PUT'), etag=etag)
        if source_header:
            resp.headers['X-Copied-From'] = quote(
                                                source_header.split('/', 2)[2])
            if 'last-modified' in source_resp.headers:
                resp.headers['X-Copied-From-Last-Modified'] = \
                    source_resp.headers['last-modified']
            for k, v in req.headers.items():
                if k.lower().startswith('x-object-meta-'):
                    resp.headers[k] = v
        resp.last_modified = float(req.headers['X-Timestamp'])
        self.app.logger.timing_since('%s.timing' % (stats_type,), start_time)
        return resp

    @public
    @delay_denial
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        start_time = time.time()
        (container_partition, containers, _junk, req.acl,
         req.environ['swift_sync_key'], object_versions) = \
            self.container_info(self.account_name, self.container_name)
        if object_versions:
            # this is a version manifest and needs to be handled differently
            lcontainer = object_versions.split('/')[0]
            prefix_len = '%03x' % len(self.object_name)
            lprefix = prefix_len + self.object_name + '/'
            last_item = None
            try:
                for last_item in self._listing_iter(lcontainer, lprefix,
                                                    req.environ):
                    pass
            except ListingIterNotFound:
                # no worries, last_item is None
                pass
            except ListingIterNotAuthorized, err:
                self.app.logger.increment('auth_short_circuits')
                return err.aresp
            except ListingIterError:
                self.app.logger.increment('errors')
                return HTTPServerError(request=req)
            if last_item:
                # there are older versions so copy the previous version to the
                # current object and delete the previous version
                orig_container = self.container_name
                orig_obj = self.object_name
                self.container_name = lcontainer
                self.object_name = last_item['name']
                copy_path = '/' + self.account_name + '/' + \
                            self.container_name + '/' + self.object_name
                copy_headers = {'X-Newest': 'True',
                                'Destination': orig_container + '/' + orig_obj
                               }
                copy_environ = {'REQUEST_METHOD': 'COPY',
                                'swift_versioned_copy': True
                               }
                creq = Request.blank(copy_path, headers=copy_headers,
                                 environ=copy_environ)
                copy_resp = self.COPY(creq)
                if is_client_error(copy_resp.status_int):
                    # some user error, maybe permissions
                    return HTTPPreconditionFailed(request=req)
                elif not is_success(copy_resp.status_int):
                    # could not copy the data, bail
                    return HTTPServiceUnavailable(request=req)
                # reset these because the COPY changed them
                self.container_name = lcontainer
                self.object_name = last_item['name']
                new_del_req = Request.blank(copy_path, environ=req.environ)
                (container_partition, containers,
                    _junk, new_del_req.acl, _junk, _junk) = \
                    self.container_info(self.account_name, self.container_name)
                new_del_req.path_info = copy_path
                req = new_del_req
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                self.app.logger.increment('auth_short_circuits')
                return aresp
        if not containers:
            self.app.logger.timing_since('DELETE.timing', start_time)
            return HTTPNotFound(request=req)
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        # Used by container sync feature
        if 'x-timestamp' in req.headers:
            try:
                req.headers['X-Timestamp'] = \
                    normalize_timestamp(float(req.headers['x-timestamp']))
            except ValueError:
                self.app.logger.increment('errors')
                return HTTPBadRequest(request=req, content_type='text/plain',
                    body='X-Timestamp should be a UNIX timestamp float value; '
                         'was %r' % req.headers['x-timestamp'])
        else:
            req.headers['X-Timestamp'] = normalize_timestamp(time.time())
        headers = []
        for container in containers:
            nheaders = dict(req.headers.iteritems())
            nheaders['Connection'] = 'close'
            nheaders['X-Container-Host'] = '%(ip)s:%(port)s' % container
            nheaders['X-Container-Partition'] = container_partition
            nheaders['X-Container-Device'] = container['device']
            headers.append(nheaders)
        resp = self.make_requests(req, self.app.object_ring,
                partition, 'DELETE', req.path_info, headers)
        self.app.logger.timing_since('DELETE.timing', start_time)
        return resp

    @public
    @delay_denial
    def COPY(self, req):
        """HTTP COPY request handler."""
        start_time = time.time()
        dest = req.headers.get('Destination')
        if not dest:
            self.app.logger.increment('errors')
            return HTTPPreconditionFailed(request=req,
                                          body='Destination header required')
        dest = unquote(dest)
        if not dest.startswith('/'):
            dest = '/' + dest
        try:
            _junk, dest_container, dest_object = dest.split('/', 2)
        except ValueError:
            self.app.logger.increment('errors')
            return HTTPPreconditionFailed(request=req,
                    body='Destination header must be of the form '
                         '<container name>/<object name>')
        source = '/' + self.container_name + '/' + self.object_name
        self.container_name = dest_container
        self.object_name = dest_object
        # re-write the existing request as a PUT instead of creating a new one
        # since this one is already attached to the posthooklogger
        req.method = 'PUT'
        req.path_info = '/' + self.account_name + dest
        req.headers['Content-Length'] = 0
        req.headers['X-Copy-From'] = quote(source)
        del req.headers['Destination']
        return self.PUT(req, start_time=start_time, stats_type='COPY')


class ContainerController(Controller):
    """WSGI controller for container requests"""
    server_type = _('Container')

    # Ensure these are all lowercase
    pass_through_headers = ['x-container-read', 'x-container-write',
                            'x-container-sync-key', 'x-container-sync-to',
                            'x-versions-location']

    def __init__(self, app, account_name, container_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)

    def clean_acls(self, req):
        if 'swift.clean_acl' in req.environ:
            for header in ('x-container-read', 'x-container-write'):
                if header in req.headers:
                    try:
                        req.headers[header] = \
                            req.environ['swift.clean_acl'](header,
                                                           req.headers[header])
                    except ValueError, err:
                        return HTTPBadRequest(request=req, body=str(err))
        return None

    def GETorHEAD(self, req, stats_type):
        """Handler for HTTP GET/HEAD requests."""
        start_time = time.time()
        if not self.account_info(self.account_name)[1]:
            self.app.logger.timing_since(
                '%s.timing' % (stats_type,), start_time)
            return HTTPNotFound(request=req)
        part, nodes = self.app.container_ring.get_nodes(
                        self.account_name, self.container_name)
        shuffle(nodes)
        resp = self.GETorHEAD_base(req, _('Container'), part, nodes,
                req.path_info, len(nodes))

        if self.app.memcache:
            # set the memcache container size for ratelimiting
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.set(cache_key,
              {'status': resp.status_int,
               'read_acl': resp.headers.get('x-container-read'),
               'write_acl': resp.headers.get('x-container-write'),
               'sync_key': resp.headers.get('x-container-sync-key'),
               'container_size': resp.headers.get('x-container-object-count'),
               'versions': resp.headers.get('x-versions-location')},
                                  timeout=self.app.recheck_container_existence)

        if 'swift.authorize' in req.environ:
            req.acl = resp.headers.get('x-container-read')
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                self.app.logger.increment('auth_short_circuits')
                return aresp
        if not req.environ.get('swift_owner', False):
            for key in ('x-container-read', 'x-container-write',
                        'x-container-sync-key', 'x-container-sync-to'):
                if key in resp.headers:
                    del resp.headers[key]
        self.app.logger.timing_since('%s.timing' % (stats_type,), start_time)
        return resp

    @public
    @delay_denial
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req, stats_type='GET')

    @public
    @delay_denial
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req, stats_type='HEAD')

    @public
    def PUT(self, req):
        """HTTP PUT request handler."""
        start_time = time.time()
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            self.app.logger.increment('errors')
            return error_response
        if len(self.container_name) > MAX_CONTAINER_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Container name length of %d longer than %d' % \
                        (len(self.container_name), MAX_CONTAINER_NAME_LENGTH)
            self.app.logger.increment('errors')
            return resp
        account_partition, accounts, container_count = \
            self.account_info(self.account_name,
                              autocreate=self.app.account_autocreate)
        if self.app.max_containers_per_account > 0 and \
                container_count >= self.app.max_containers_per_account and \
                self.account_name not in self.app.max_containers_whitelist:
            resp = HTTPForbidden(request=req)
            resp.body = 'Reached container limit of %s' % \
                        self.app.max_containers_per_account
            return resp
        if not accounts:
            self.app.logger.timing_since('PUT.timing', start_time)
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = []
        for account in accounts:
            nheaders = {'X-Timestamp': normalize_timestamp(time.time()),
                        'x-trans-id': self.trans_id,
                        'X-Account-Host': '%(ip)s:%(port)s' % account,
                        'X-Account-Partition': account_partition,
                        'X-Account-Device': account['device'],
                        'Connection': 'close'}
            self.transfer_headers(req.headers, nheaders)
            headers.append(nheaders)
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        resp = self.make_requests(req, self.app.container_ring,
                container_partition, 'PUT', req.path_info, headers)
        self.app.logger.timing_since('PUT.timing', start_time)
        return resp

    @public
    def POST(self, req):
        """HTTP POST request handler."""
        start_time = time.time()
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            self.app.logger.increment('errors')
            return error_response
        account_partition, accounts, container_count = \
            self.account_info(self.account_name,
                              autocreate=self.app.account_autocreate)
        if not accounts:
            self.app.logger.timing_since('POST.timing', start_time)
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'x-trans-id': self.trans_id,
                   'Connection': 'close'}
        self.transfer_headers(req.headers, headers)
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        resp = self.make_requests(req, self.app.container_ring,
                container_partition, 'POST', req.path_info,
                [headers] * len(containers))
        self.app.logger.timing_since('POST.timing', start_time)
        return resp

    @public
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        start_time = time.time()
        account_partition, accounts, container_count = \
            self.account_info(self.account_name)
        if not accounts:
            self.app.logger.timing_since('DELETE.timing', start_time)
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = []
        for account in accounts:
            headers.append({'X-Timestamp': normalize_timestamp(time.time()),
                           'X-Trans-Id': self.trans_id,
                           'X-Account-Host': '%(ip)s:%(port)s' % account,
                           'X-Account-Partition': account_partition,
                           'X-Account-Device': account['device'],
                           'Connection': 'close'})
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        resp = self.make_requests(req, self.app.container_ring,
                    container_partition, 'DELETE', req.path_info, headers)
        # Indicates no server had the container
        self.app.logger.timing_since('DELETE.timing', start_time)
        if resp.status_int == HTTP_ACCEPTED:
            return HTTPNotFound(request=req)
        return resp


class AccountController(Controller):
    """WSGI controller for account requests"""
    server_type = _('Account')

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)

    def GETorHEAD(self, req, stats_type):
        """Handler for HTTP GET/HEAD requests."""
        start_time = time.time()
        partition, nodes = self.app.account_ring.get_nodes(self.account_name)
        shuffle(nodes)
        resp = self.GETorHEAD_base(req, _('Account'), partition, nodes,
                req.path_info.rstrip('/'), len(nodes))
        if resp.status_int == HTTP_NOT_FOUND and self.app.account_autocreate:
            if len(self.account_name) > MAX_ACCOUNT_NAME_LENGTH:
                resp = HTTPBadRequest(request=req)
                resp.body = 'Account name length of %d longer than %d' % \
                            (len(self.account_name), MAX_ACCOUNT_NAME_LENGTH)
                self.app.logger.timing_since(
                    '%s.timing' % (stats_type,), start_time)
                return resp
            headers = {'X-Timestamp': normalize_timestamp(time.time()),
                       'X-Trans-Id': self.trans_id,
                       'Connection': 'close'}
            resp = self.make_requests(
                Request.blank('/v1/' + self.account_name),
                self.app.account_ring, partition, 'PUT',
                '/' + self.account_name, [headers] * len(nodes))
            if not is_success(resp.status_int):
                raise Exception('Could not autocreate account %r' %
                                self.account_name)
            resp = self.GETorHEAD_base(req, _('Account'), partition, nodes,
                req.path_info.rstrip('/'), len(nodes))
        self.app.logger.timing_since('%s.timing' % (stats_type,), start_time)
        return resp

    @public
    def PUT(self, req):
        """HTTP PUT request handler."""
        start_time = time.time()
        if not self.app.allow_account_management:
            self.app.logger.timing_since('PUT.timing', start_time)
            return HTTPMethodNotAllowed(request=req)
        error_response = check_metadata(req, 'account')
        if error_response:
            self.app.logger.increment('errors')
            return error_response
        if len(self.account_name) > MAX_ACCOUNT_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Account name length of %d longer than %d' % \
                        (len(self.account_name), MAX_ACCOUNT_NAME_LENGTH)
            self.app.logger.increment('errors')
            return resp
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'x-trans-id': self.trans_id,
                   'Connection': 'close'}
        self.transfer_headers(req.headers, headers)
        if self.app.memcache:
            self.app.memcache.delete('account%s' % req.path_info.rstrip('/'))
        resp = self.make_requests(req, self.app.account_ring,
            account_partition, 'PUT', req.path_info, [headers] * len(accounts))
        self.app.logger.timing_since('PUT.timing', start_time)
        return resp

    @public
    def POST(self, req):
        """HTTP POST request handler."""
        start_time = time.time()
        error_response = check_metadata(req, 'account')
        if error_response:
            self.app.logger.increment('errors')
            return error_response
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'X-Trans-Id': self.trans_id,
                   'Connection': 'close'}
        self.transfer_headers(req.headers, headers)
        if self.app.memcache:
            self.app.memcache.delete('account%s' % req.path_info.rstrip('/'))
        resp = self.make_requests(req, self.app.account_ring,
            account_partition, 'POST', req.path_info,
            [headers] * len(accounts))
        if resp.status_int == HTTP_NOT_FOUND and self.app.account_autocreate:
            if len(self.account_name) > MAX_ACCOUNT_NAME_LENGTH:
                resp = HTTPBadRequest(request=req)
                resp.body = 'Account name length of %d longer than %d' % \
                            (len(self.account_name), MAX_ACCOUNT_NAME_LENGTH)
                self.app.logger.increment('errors')
                return resp
            resp = self.make_requests(
                Request.blank('/v1/' + self.account_name),
                self.app.account_ring, account_partition, 'PUT',
                '/' + self.account_name, [headers] * len(accounts))
            if not is_success(resp.status_int):
                raise Exception('Could not autocreate account %r' %
                                self.account_name)
        self.app.logger.timing_since('POST.timing', start_time)
        return resp

    @public
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        start_time = time.time()
        if not self.app.allow_account_management:
            self.app.logger.timing_since('DELETE.timing', start_time)
            return HTTPMethodNotAllowed(request=req)
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'X-Trans-Id': self.trans_id,
                   'Connection': 'close'}
        if self.app.memcache:
            self.app.memcache.delete('account%s' % req.path_info.rstrip('/'))
        resp = self.make_requests(req, self.app.account_ring,
            account_partition, 'DELETE', req.path_info,
            [headers] * len(accounts))
        self.app.logger.timing_since('DELETE.timing', start_time)
        return resp


class BaseApplication(object):
    """Base WSGI application for the proxy server"""

    def __init__(self, conf, memcache=None, logger=None, account_ring=None,
                 container_ring=None, object_ring=None):
        if conf is None:
            conf = {}
        if logger is None:
            self.logger = get_logger(conf, log_route='proxy-server')
            access_log_conf = {}
            for key in ('log_facility', 'log_name', 'log_level',
                        'log_udp_host', 'log_udp_port'):
                value = conf.get('access_' + key, conf.get(key, None))
                if value:
                    access_log_conf[key] = value
            self.access_logger = get_logger(access_log_conf,
                                            log_route='proxy-access')
        else:
            self.logger = self.access_logger = logger

        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.client_timeout = int(conf.get('client_timeout', 60))
        self.put_queue_depth = int(conf.get('put_queue_depth', 10))
        self.object_chunk_size = int(conf.get('object_chunk_size', 65536))
        self.client_chunk_size = int(conf.get('client_chunk_size', 65536))
        self.log_headers = conf.get('log_headers', 'no').lower() in TRUE_VALUES
        self.error_suppression_interval = \
            int(conf.get('error_suppression_interval', 60))
        self.error_suppression_limit = \
            int(conf.get('error_suppression_limit', 10))
        self.recheck_container_existence = \
            int(conf.get('recheck_container_existence', 60))
        self.recheck_account_existence = \
            int(conf.get('recheck_account_existence', 60))
        self.allow_account_management = \
            conf.get('allow_account_management', 'no').lower() in TRUE_VALUES
        self.object_post_as_copy = \
            conf.get('object_post_as_copy', 'true').lower() in TRUE_VALUES
        self.resellers_conf = ConfigParser()
        self.resellers_conf.read(os.path.join(swift_dir, 'resellers.conf'))
        self.object_ring = object_ring or Ring(swift_dir, ring_name='object')
        self.container_ring = container_ring or Ring(swift_dir,
                ring_name='container')
        self.account_ring = account_ring or Ring(swift_dir,
                ring_name='account')
        self.memcache = memcache
        mimetypes.init(mimetypes.knownfiles +
                       [os.path.join(swift_dir, 'mime.types')])
        self.account_autocreate = \
            conf.get('account_autocreate', 'no').lower() in TRUE_VALUES
        self.expiring_objects_account = \
            (conf.get('auto_create_account_prefix') or '.') + \
            'expiring_objects'
        self.expiring_objects_container_divisor = \
            int(conf.get('expiring_objects_container_divisor') or 86400)
        self.max_containers_per_account = \
            int(conf.get('max_containers_per_account') or 0)
        self.max_containers_whitelist = [a.strip()
            for a in conf.get('max_containers_whitelist', '').split(',')
            if a.strip()]
        self.deny_host_headers = [host.strip() for host in
            conf.get('deny_host_headers', '').split(',') if host.strip()]
        self.rate_limit_after_segment = \
            int(conf.get('rate_limit_after_segment', 10))
        self.rate_limit_segments_per_sec = \
            int(conf.get('rate_limit_segments_per_sec', 1))
        self.log_handoffs = \
            conf.get('log_handoffs', 'true').lower() in TRUE_VALUES

    def get_controller(self, path):
        """
        Get the controller to handle a request.

        :param path: path from request
        :returns: tuple of (controller class, path dictionary)

        :raises: ValueError (thrown by split_path) if given invalid path
        """
        version, account, container, obj = split_path(path, 1, 4, True)
        d = dict(version=version,
                account_name=account,
                container_name=container,
                object_name=obj)
        if obj and container and account:
            return ObjectController, d
        elif container and account:
            return ContainerController, d
        elif account and not container and not obj:
            return AccountController, d
        return None, d

    def __call__(self, env, start_response):
        """
        WSGI entry point.
        Wraps env in webob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            if self.memcache is None:
                self.memcache = cache_from_env(env)
            req = self.update_request(Request(env))
            return self.handle_request(req)(env, start_response)
        except UnicodeError:
            err = HTTPPreconditionFailed(request=req, body='Invalid UTF8')
            return err(env, start_response)
        except (Exception, Timeout):
            start_response('500 Server Error',
                    [('Content-Type', 'text/plain')])
            return ['Internal server error.\n']

    def update_request(self, req):
        if 'x-storage-token' in req.headers and \
                'x-auth-token' not in req.headers:
            req.headers['x-auth-token'] = req.headers['x-storage-token']
        return req

    def handle_request(self, req):
        """
        Entry point for proxy server.
        Should return a WSGI-style callable (such as webob.Response).

        :param req: webob.Request object
        """
        try:
            self.logger.set_statsd_prefix('proxy-server')
            if req.content_length and req.content_length < 0:
                self.logger.increment('errors')
                return HTTPBadRequest(request=req,
                                      body='Invalid Content-Length')

            try:
                if not check_utf8(req.path_info):
                    self.logger.increment('errors')
                    return HTTPPreconditionFailed(request=req,
                                                  body='Invalid UTF8')
            except UnicodeError:
                self.logger.increment('errors')
                return HTTPPreconditionFailed(request=req, body='Invalid UTF8')

            try:
                controller, path_parts = self.get_controller(req.path)
                p = req.path_info
                if isinstance(p, unicode):
                    p = p.encode('utf-8')
            except ValueError:
                self.logger.increment('errors')
                return HTTPNotFound(request=req)
            if not controller:
                self.logger.increment('errors')
                return HTTPPreconditionFailed(request=req, body='Bad URL')
            if self.deny_host_headers and \
                    req.host.split(':')[0] in self.deny_host_headers:
                return HTTPForbidden(request=req, body='Invalid host header')

            self.logger.set_statsd_prefix('proxy-server.' +
                                          controller.server_type)
            controller = controller(self, **path_parts)
            if 'swift.trans_id' not in req.environ:
                # if this wasn't set by an earlier middleware, set it now
                trans_id = 'tx' + uuid.uuid4().hex
                req.environ['swift.trans_id'] = trans_id
                self.logger.txn_id = trans_id
            req.headers['x-trans-id'] = req.environ['swift.trans_id']
            controller.trans_id = req.environ['swift.trans_id']
            self.logger.client_ip = get_remote_client(req)
            try:
                handler = getattr(controller, req.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                self.logger.increment('method_not_allowed')
                return HTTPMethodNotAllowed(request=req)
            if path_parts['version']:
                req.path_info_pop()
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
                        self.logger.increment('auth_short_circuits')
                        return resp
            return handler(req)
        except (Exception, Timeout):
            self.logger.exception(_('ERROR Unhandled exception in request'))
            return HTTPServerError(request=req)


class Application(BaseApplication):
    """WSGI application for the proxy server."""

    def handle_request(self, req):
        """
        Wraps the BaseApplication.handle_request and logs the request.
        """
        req.start_time = time.time()
        req.response = super(Application, self).handle_request(req)
        return req.response


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    return Application(conf)
