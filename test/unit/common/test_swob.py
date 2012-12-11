# Copyright (c) 2012 OpenStack, LLC.
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

"Tests for swift.common.swob"

import unittest
import datetime
from StringIO import StringIO

import swift.common.swob


class TestHeaderEnvironProxy(unittest.TestCase):
    def test_proxy(self):
        environ = {}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        proxy['Content-Length'] = 20
        proxy['Content-Type'] = 'text/plain'
        proxy['Something-Else'] = 'somevalue'
        self.assertEquals(
            proxy.environ, {'CONTENT_LENGTH': '20',
                            'CONTENT_TYPE': 'text/plain',
                            'HTTP_SOMETHING_ELSE': 'somevalue'})
        self.assertEquals(proxy['content-length'], '20')
        self.assertEquals(proxy['content-type'], 'text/plain')
        self.assertEquals(proxy['something-else'], 'somevalue')

    def test_del(self):
        environ = {}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        proxy['Content-Length'] = 20
        proxy['Content-Type'] = 'text/plain'
        proxy['Something-Else'] = 'somevalue'
        del proxy['Content-Length']
        del proxy['Content-Type']
        del proxy['Something-Else']
        self.assertEquals(proxy.environ, {})

    def test_contains(self):
        environ = {}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        proxy['Content-Length'] = 20
        proxy['Content-Type'] = 'text/plain'
        proxy['Something-Else'] = 'somevalue'
        self.assert_('content-length' in proxy)
        self.assert_('content-type' in proxy)
        self.assert_('something-else' in proxy)

    def test_keys(self):
        environ = {}
        proxy = swift.common.swob.HeaderEnvironProxy(environ)
        proxy['Content-Length'] = 20
        proxy['Content-Type'] = 'text/plain'
        proxy['Something-Else'] = 'somevalue'
        self.assertEquals(
            set(proxy.keys()),
            set(('Content-Length', 'Content-Type', 'Something-Else')))


class TestHeaderKeyDict(unittest.TestCase):
    def test_case_insensitive(self):
        headers = swift.common.swob.HeaderKeyDict()
        headers['Content-Length'] = 0
        headers['CONTENT-LENGTH'] = 10
        headers['content-length'] = 20
        self.assertEquals(headers['Content-Length'], '20')
        self.assertEquals(headers['content-length'], '20')
        self.assertEquals(headers['CONTENT-LENGTH'], '20')

    def test_del_contains(self):
        headers = swift.common.swob.HeaderKeyDict()
        headers['Content-Length'] = 0
        self.assert_('Content-Length' in headers)
        del headers['Content-Length']
        self.assert_('Content-Length' not in headers)

    def test_update(self):
        headers = swift.common.swob.HeaderKeyDict()
        headers.update({'Content-Length': '0'})
        headers.update([('Content-Type', 'text/plain')])
        self.assertEquals(headers['Content-Length'], '0')
        self.assertEquals(headers['Content-Type'], 'text/plain')

    def test_get(self):
        headers = swift.common.swob.HeaderKeyDict()
        headers['content-length'] = 20
        self.assertEquals(headers.get('CONTENT-LENGTH'), '20')
        self.assertEquals(headers.get('something-else'), None)
        self.assertEquals(headers.get('something-else', True), True)


class TestRange(unittest.TestCase):
    def test_range(self):
        range = swift.common.swob.Range('bytes=1-7')
        self.assertEquals(range.ranges[0], (1, 7))

    def test_upsidedown_range(self):
        range = swift.common.swob.Range('bytes=5-10')
        self.assertEquals(range.range_for_length(2), None)

    def test_str(self):
        for range_str in ('bytes=1-7', 'bytes=1-', 'bytes=-1',
                          'bytes=1-7,9-12', 'bytes=-7,9-'):
            range = swift.common.swob.Range(range_str)
            self.assertEquals(str(range), range_str)

    def test_range_for_length(self):
        range = swift.common.swob.Range('bytes=1-7')
        self.assertEquals(range.range_for_length(10), (1, 8))
        self.assertEquals(range.range_for_length(5), (1, 5))
        self.assertEquals(range.range_for_length(None), None)

    def test_range_for_length_no_end(self):
        range = swift.common.swob.Range('bytes=1-')
        self.assertEquals(range.range_for_length(10), (1, 10))
        self.assertEquals(range.range_for_length(5), (1, 5))
        self.assertEquals(range.range_for_length(None), None)
        # This used to freak out:
        range = swift.common.swob.Range('bytes=100-')
        self.assertEquals(range.range_for_length(5), None)
        self.assertEquals(range.range_for_length(None), None)

    def test_range_for_length_no_start(self):
        range = swift.common.swob.Range('bytes=-7')
        self.assertEquals(range.range_for_length(10), (3, 10))
        self.assertEquals(range.range_for_length(5), (0, 5))
        self.assertEquals(range.range_for_length(None), None)

    def test_range_invalid_syntax(self):
        range = swift.common.swob.Range('bytes=10-2')
        self.assertEquals(range.ranges, [])


class TestMatch(unittest.TestCase):
    def test_match(self):
        match = swift.common.swob.Match('"a", "b"')
        self.assertEquals(match.tags, set(('a', 'b')))
        self.assert_('a' in match)
        self.assert_('b' in match)
        self.assert_('c' not in match)

    def test_match_star(self):
        match = swift.common.swob.Match('"a", "*"')
        self.assert_('a' in match)
        self.assert_('b' in match)
        self.assert_('c' in match)

    def test_match_noquote(self):
        match = swift.common.swob.Match('a, b')
        self.assertEquals(match.tags, set(('a', 'b')))
        self.assert_('a' in match)
        self.assert_('b' in match)
        self.assert_('c' not in match)


class TestAccept(unittest.TestCase):
    def test_accept_json(self):
        for accept in ('application/json', 'application/json;q=1.0,*/*;q=0.9',
                       '*/*;q=0.9,application/json;q=1.0', 'application/*'):
            acc = swift.common.swob.Accept(accept)
            match = acc.best_match(['text/plain', 'application/json',
                                    'application/xml', 'text/xml'],
                                   default_match='text/plain')
            self.assertEquals(match, 'application/json')

    def test_accept_plain(self):
        for accept in ('', 'text/plain', 'application/xml;q=0.8,*/*;q=0.9',
                       '*/*;q=0.9,application/xml;q=0.8', '*/*',
                       'text/plain,application/xml'):
            acc = swift.common.swob.Accept(accept)
            match = acc.best_match(['text/plain', 'application/json',
                                    'application/xml', 'text/xml'],
                                   default_match='text/plain')
            self.assertEquals(match, 'text/plain')

    def test_accept_xml(self):
        for accept in ('application/xml', 'application/xml;q=1.0,*/*;q=0.9',
                       '*/*;q=0.9,application/xml;q=1.0'):
            acc = swift.common.swob.Accept(accept)
            match = acc.best_match(['text/plain', 'application/xml',
                                   'text/xml'], default_match='text/plain')
            self.assertEquals(match, 'application/xml')


class TestRequest(unittest.TestCase):
    def test_blank(self):
        req = swift.common.swob.Request.blank(
            '/', environ={'REQUEST_METHOD': 'POST'},
            headers={'Content-Type': 'text/plain'}, body='hi')
        self.assertEquals(req.path_info, '/')
        self.assertEquals(req.body, 'hi')
        self.assertEquals(req.headers['Content-Type'], 'text/plain')
        self.assertEquals(req.method, 'POST')

    def test_blank_body_precedence(self):
        req = swift.common.swob.Request.blank(
            '/', environ={'REQUEST_METHOD': 'POST',
                          'wsgi.input': StringIO('')},
            headers={'Content-Type': 'text/plain'}, body='hi')
        self.assertEquals(req.path_info, '/')
        self.assertEquals(req.body, 'hi')
        self.assertEquals(req.headers['Content-Type'], 'text/plain')
        self.assertEquals(req.method, 'POST')

    def test_params(self):
        req = swift.common.swob.Request.blank('/?a=b&c=d')
        self.assertEquals(req.params['a'], 'b')
        self.assertEquals(req.params['c'], 'd')

    def test_path(self):
        req = swift.common.swob.Request.blank('/hi?a=b&c=d')
        self.assertEquals(req.path, '/hi')
        req = swift.common.swob.Request.blank(
            '/', environ={'SCRIPT_NAME': '/hi', 'PATH_INFO': '/there'})
        self.assertEquals(req.path, '/hi/there')

    def test_path_info_pop(self):
        req = swift.common.swob.Request.blank('/hi/there')
        self.assertEquals(req.path_info_pop(), 'hi')
        self.assertEquals(req.path_info, '/there')
        self.assertEquals(req.script_name, '/hi')

    def test_bad_path_info_pop(self):
        req = swift.common.swob.Request.blank('blahblah')
        self.assertEquals(req.path_info_pop(), None)

    def test_copy_get(self):
        req = swift.common.swob.Request.blank(
            '/hi/there', environ={'REQUEST_METHOD': 'POST'})
        self.assertEquals(req.method, 'POST')
        req2 = req.copy_get()
        self.assertEquals(req2.method, 'GET')

    def test_get_response(self):
        def test_app(environ, start_response):
            start_response('200 OK', [])
            return ['hi']

        req = swift.common.swob.Request.blank('/')
        resp = req.get_response(test_app)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, 'hi')

    def test_properties(self):
        req = swift.common.swob.Request.blank('/hi/there', body='hi')

        self.assertEquals(req.body, 'hi')
        self.assertEquals(req.content_length, 2)

        req.remote_addr = 'something'
        self.assertEquals(req.environ['REMOTE_ADDR'], 'something')
        req.body = 'whatever'
        self.assertEquals(req.content_length, 8)
        self.assertEquals(req.body, 'whatever')
        self.assertEquals(req.method, 'GET')

        req.range = 'bytes=1-7'
        self.assertEquals(req.range.ranges[0], (1, 7))

        self.assert_('Range' in req.headers)
        req.range = None
        self.assert_('Range' not in req.headers)

    def test_datetime_properties(self):
        req = swift.common.swob.Request.blank('/hi/there', body='hi')

        req.if_unmodified_since = 0
        self.assert_(isinstance(req.if_unmodified_since, datetime.datetime))
        if_unmodified_since = req.if_unmodified_since
        req.if_unmodified_since = if_unmodified_since
        self.assertEquals(if_unmodified_since, req.if_unmodified_since)

        req.if_unmodified_since = 'something'
        self.assertEquals(req.headers['If-Unmodified-Since'], 'something')
        self.assertEquals(req.if_unmodified_since, None)

        req.if_unmodified_since = -1
        self.assertRaises(ValueError, lambda: req.if_unmodified_since)

        self.assert_('If-Unmodified-Since' in req.headers)
        req.if_unmodified_since = None
        self.assert_('If-Unmodified-Since' not in req.headers)

    def test_bad_range(self):
        req = swift.common.swob.Request.blank('/hi/there', body='hi')
        req.range = 'bad range'
        self.assertEquals(req.range, None)

    def test_accept_header(self):
        req = swift.common.swob.Request({'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/',
                                         'HTTP_ACCEPT': 'application/json'})
        self.assertEqual(
            req.accept.best_match(['application/json', 'text/plain']),
            'application/json')
        self.assertEqual(
            req.accept.best_match(['text/plain', 'application/json']),
            'application/json')

    def test_path_qs(self):
        req = swift.common.swob.Request.blank('/hi/there?hello=equal&acl')
        self.assertEqual(req.path_qs, '/hi/there?hello=equal&acl')

        req = swift.common.swob.Request({'PATH_INFO': '/hi/there',
                                         'QUERY_STRING': 'hello=equal&acl'})
        self.assertEqual(req.path_qs, '/hi/there?hello=equal&acl')


class TestStatusMap(unittest.TestCase):
    def test_status_map(self):
        response_args = []

        def start_response(status, headers):
            response_args.append(status)
            response_args.append(headers)
        resp_cls = swift.common.swob.status_map[404]
        resp = resp_cls()
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(resp.title, 'Not Found')
        body = ''.join(resp({}, start_response))
        self.assert_('The resource could not be found.' in body)
        self.assertEquals(response_args[0], '404 Not Found')
        headers = dict(response_args[1])
        self.assertEquals(headers['content-type'], 'text/html')
        self.assert_(int(headers['content-length']) > 0)


class TestResponse(unittest.TestCase):
    def _get_response(self):
        def test_app(environ, start_response):
            start_response('200 OK', [])
            return ['hi']

        req = swift.common.swob.Request.blank('/')
        return req.get_response(test_app)

    def test_properties(self):
        resp = self._get_response()

        resp.location = 'something'
        self.assertEquals(resp.location, 'something')
        self.assert_('Location' in resp.headers)
        resp.location = None
        self.assert_('Location' not in resp.headers)

        resp.content_type = 'text/plain'
        self.assert_('Content-Type' in resp.headers)
        resp.content_type = None
        self.assert_('Content-Type' not in resp.headers)

    def test_empty_body(self):
        resp = self._get_response()
        resp.body = ''
        self.assertEquals(resp.body, '')

    def test_unicode_body(self):
        resp = self._get_response()
        resp.body = u'\N{SNOWMAN}'
        self.assertEquals(resp.body, u'\N{SNOWMAN}'.encode('utf-8'))

    def test_location_rewrite(self):
        def start_response(env, headers):
            pass
        req = swift.common.swob.Request.blank('/')
        resp = self._get_response()
        resp.location = '/something'
        body = ''.join(resp(req.environ, start_response))
        self.assertEquals(resp.location, 'http://localhost/something')

    def test_app_iter(self):
        def start_response(env, headers):
            pass
        resp = self._get_response()
        resp.app_iter = ['a', 'b', 'c']
        body = ''.join(resp({}, start_response))
        self.assertEquals(body, 'abc')

    def test_range_body(self):

        def test_app(environ, start_response):
            start_response('200 OK', [('Content-Length', '10')])
            return ['1234567890']

        def start_response(env, headers):
            pass

        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=1-3'})
        resp = req.get_response(test_app)
        resp.conditional_response = True
        body = ''.join(resp([], start_response))
        self.assertEquals(body, '234')
        self.assertEquals(resp.status, '206 Partial Content')

        resp = swift.common.swob.Response(
            body='1234567890', request=req,
            conditional_response=True)
        body = ''.join(resp([], start_response))
        self.assertEquals(body, '234')
        self.assertEquals(resp.status, '206 Partial Content')

        # No body for 416
        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=-0'})
        resp = req.get_response(test_app)
        resp.conditional_response = True
        body = ''.join(resp([], start_response))
        self.assertEquals(body, '')
        self.assertEquals(resp.content_length, 0)
        self.assertEquals(resp.status, '416 Requested Range Not Satisfiable')

        resp = swift.common.swob.Response(
            body='1234567890', request=req,
            conditional_response=True)
        body = ''.join(resp([], start_response))
        self.assertEquals(body, '')
        self.assertEquals(resp.status, '416 Requested Range Not Satisfiable')

        # Syntactically-invalid Range headers "MUST" be ignored
        req = swift.common.swob.Request.blank(
            '/', headers={'Range': 'bytes=3-2'})
        resp = req.get_response(test_app)
        resp.conditional_response = True
        body = ''.join(resp([], start_response))
        self.assertEquals(body, '1234567890')
        self.assertEquals(resp.status, '200 OK')

        resp = swift.common.swob.Response(
            body='1234567890', request=req,
            conditional_response=True)
        body = ''.join(resp([], start_response))
        self.assertEquals(body, '1234567890')
        self.assertEquals(resp.status, '200 OK')

    def test_content_type(self):
        resp = self._get_response()
        resp.content_type = 'text/plain; charset=utf8'
        self.assertEquals(resp.content_type, 'text/plain')

    def test_charset(self):
        resp = self._get_response()
        resp.content_type = 'text/plain; charset=utf8'
        self.assertEquals(resp.charset, 'utf8')
        resp.charset = 'utf16'
        self.assertEquals(resp.charset, 'utf16')

    def test_etag(self):
        resp = self._get_response()
        resp.etag = 'hi'
        self.assertEquals(resp.headers['Etag'], '"hi"')
        self.assertEquals(resp.etag, 'hi')

        self.assert_('etag' in resp.headers)
        resp.etag = None
        self.assert_('etag' not in resp.headers)


class TestUTC(unittest.TestCase):
    def test_tzname(self):
        self.assertEquals(swift.common.swob.UTC.tzname(None), 'UTC')


if __name__ == '__main__':
    unittest.main()
