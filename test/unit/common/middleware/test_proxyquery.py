from __future__ import with_statement
import unittest
import os
import cPickle as pickle
from time import time, sleep
from webob import Request
from hashlib import md5
from test.unit import connect_tcp, readuntil2crlfs
from tempfile import mkstemp, mkdtemp
from shutil import rmtree

from nose import SkipTest
from httplib import HTTPException
from webob.exc import HTTPNotFound, HTTPUnauthorized
from eventlet import sleep, spawn, Timeout, util, wsgi, listen
from gzip import GzipFile
from contextlib import contextmanager

from swift.proxy import server as proxy_server
from swift.account import server as account_server
from swift.container import server as container_server
from swift.obj import server as object_server
from swift.common.utils import mkdirs, normalize_timestamp, NullLogger
from swift.common.wsgi import monkey_patch_mimetools
from swift.common import ring
from test.unit.proxy.test_server import fake_http_connect, save_globals, \
    FakeRing, FakeMemcache, FakeMemcacheReturnsNone
from swift.common.middleware import proxyquery, objectquery

def setup():
    global _testdir, _test_servers, _test_sockets,\
    _orig_container_listing_limit, _test_coros
    monkey_patch_mimetools()
    # Since we're starting up a lot here, we're going to test more than
    # just chunked puts; we're also going to test parts of
    # proxy_server.Application we couldn't get to easily otherwise.
    _testdir = os.path.join(mkdtemp(), 'tmp_test_proxy_server_chunked')
    mkdirs(_testdir)
    rmtree(_testdir)
    mkdirs(os.path.join(_testdir, 'sda1'))
    mkdirs(os.path.join(_testdir, 'sda1', 'tmp'))
    mkdirs(os.path.join(_testdir, 'sdb1'))
    mkdirs(os.path.join(_testdir, 'sdb1', 'tmp'))
    _orig_container_listing_limit = proxy_server.CONTAINER_LISTING_LIMIT
    conf = {'devices': _testdir, 'swift_dir': _testdir,
            'mount_check': 'false', 'allowed_headers':
            'content-encoding, x-object-manifest, content-disposition, foo'}
    prolis = listen(('localhost', 0))
    acc1lis = listen(('localhost', 0))
    acc2lis = listen(('localhost', 0))
    con1lis = listen(('localhost', 0))
    con2lis = listen(('localhost', 0))
    obj1lis = listen(('localhost', 0))
    obj2lis = listen(('localhost', 0))
    _test_sockets =\
    (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis)
    pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
        [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
          'port': acc1lis.getsockname()[1]},
                {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                 'port': acc2lis.getsockname()[1]}], 30),
        GzipFile(os.path.join(_testdir, 'account.ring.gz'), 'wb'))
    pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
        [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
          'port': con1lis.getsockname()[1]},
                {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                 'port': con2lis.getsockname()[1]}], 30),
        GzipFile(os.path.join(_testdir, 'container.ring.gz'), 'wb'))
    pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
        [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
          'port': obj1lis.getsockname()[1]},
                {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                 'port': obj2lis.getsockname()[1]}], 30),
        GzipFile(os.path.join(_testdir, 'object.ring.gz'), 'wb'))
    prosrv = proxyquery.ProxyQueryMiddleware(proxy_server.Application(conf, FakeMemcacheReturnsNone()), conf)
    acc1srv = account_server.AccountController(conf)
    acc2srv = account_server.AccountController(conf)
    con1srv = container_server.ContainerController(conf)
    con2srv = container_server.ContainerController(conf)
    obj1srv = objectquery.ObjectQueryMiddleware(object_server.ObjectController(conf), conf)
    obj2srv = objectquery.ObjectQueryMiddleware(object_server.ObjectController(conf), conf)
    _test_servers =\
    (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv, obj2srv)
    nl = NullLogger()
    prospa = spawn(wsgi.server, prolis, prosrv, nl)
    acc1spa = spawn(wsgi.server, acc1lis, acc1srv, nl)
    acc2spa = spawn(wsgi.server, acc2lis, acc2srv, nl)
    con1spa = spawn(wsgi.server, con1lis, con1srv, nl)
    con2spa = spawn(wsgi.server, con2lis, con2srv, nl)
    obj1spa = spawn(wsgi.server, obj1lis, obj1srv, nl)
    obj2spa = spawn(wsgi.server, obj2lis, obj2srv, nl)
    _test_coros =\
    (prospa, acc1spa, acc2spa, con1spa, con2spa, obj1spa, obj2spa)
    # Create account
    ts = normalize_timestamp(time())
    partition, nodes = prosrv.app.account_ring.get_nodes('a')
    for node in nodes:
        conn = proxy_server.http_connect(node['ip'], node['port'],
            node['device'], partition, 'PUT', '/a',
                {'X-Timestamp': ts, 'x-trans-id': 'test'})
        resp = conn.getresponse()
        assert(resp.status == 201)
        # Create container
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    fd.write('PUT /v1/a/c HTTP/1.1\r\nHost: localhost\r\n'
             'Connection: close\r\nX-Auth-Token: t\r\n'
             'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = 'HTTP/1.1 201'
    assert(headers[:len(exp)] == exp)

def teardown():
    for server in _test_coros:
        server.kill()
    proxy_server.CONTAINER_LISTING_LIMIT = _orig_container_listing_limit
    rmtree(os.path.dirname(_testdir))


class TestProxyQuery(unittest.TestCase):

    def setUp(self):
#        self.proxy_app = proxy_server.Application(None, FakeMemcache(),
#            account_ring=FakeRing(), container_ring=FakeRing(),
#            object_ring=FakeRing())
#        self.conf = {'devices': _testdir, 'swift_dir': _testdir,
#                'mount_check': 'false', 'allowed_headers':
#                'content-encoding, x-object-manifest, content-disposition, foo'}
#        self.app = proxyquery.ProxyQueryMiddleware(self.proxy_app, self.conf)
        monkey_patch_mimetools()

    def tearDown(self):
        proxy_server.CONTAINER_LISTING_LIMIT = _orig_container_listing_limit

    def setup_QUERY(self):
        def get_zerovm_mock():
            return ('from sys import argv \nif len(argv) \x3C 2 or len(argv) \x3E 5:\n    raise Exception(\'Incorrect number of arguments\')\nif argv[1] != \'-Y2\':\n    raise Exception(\'Invalid first argument: %s\' % sys.argv[1])\nif argv[2][:2] != \'-M\':\n    raise Exception(\'Invalid second argument: %s\' % sys.argv[2])\nmanifest = argv[2][2:]\nprint \'manifest file:%s\\n\' % manifest \ninputmnfst = file(manifest, \'r\').readlines()\nprint \'manifest:\\n%s\\n\' % inputmnfst\nclass Mnfst:\n    pass\nmnfst = Mnfst()\nindex = 0\nstatus = \'ok.\' if len(argv) \x3C 3 else argv[3]\nprint \'param status:\' + str(status) + \'\\n\'\nretcode = 0 if len(argv) \x3C 4 else argv[4]\nprint \'param retcode:\' + str(retcode) + \'\\n\'\ndef retrieve_mnfst_field(n, vv = None, isint=False, rgx=None):\n    global index                  \n    if len(inputmnfst) \x3C index:\n        raise Exception(\'missing  %s\' % n)\n    if not inputmnfst[index][0:19] == n:\n        raise Exception(\'expecting %s got %s\' \n                        % (n, inputmnfst[index][0:19]))       \n    if not inputmnfst[index][19] == \'=\':\n        raise Exception(\'missing \\\'=\\\' at %s got %s\' \n                        % (n, inputmnfst[index][19]))       \n    v = inputmnfst[index][20:-1]\n    if isint:\n        v = int(v)\n    if rgx:\n        import re\n        if not re.match(rgx,v):\n            raise Exception(\'mnfst: %s does not match %s\' % n, rgx)\n    if n[0] == \'?\':\n        n = n[1:]\n    if vv and vv != v:\n        raise Exception(\'for %s expected %s got %s\' % (n, vv, v))\n    index += 1\n    print n + \'=\' + str(v) + \'\\n\'       \n    setattr(mnfst, n.strip(), v)\nretrieve_mnfst_field(\'version            \',\'11nov2011\')\nretrieve_mnfst_field(\'zerovm             \')\nretrieve_mnfst_field(\'nexe               \')\nretrieve_mnfst_field(\'maxnexe            \', 256*1048576, True)\nretrieve_mnfst_field(\'input              \')\nretrieve_mnfst_field(\'maxinput           \', 256*1048576, True)\nretrieve_mnfst_field(\'#etag              \')\nretrieve_mnfst_field(\'#content-type      \')\nretrieve_mnfst_field(\'#x-timestamp       \')\nretrieve_mnfst_field(\'#x-data-attr       \',\'Message:Hello\')\nretrieve_mnfst_field(\'#x-data-attr       \',\'Base:10\')\nretrieve_''mnfst_field(\'#x-data-attr       \',\'Format:Pickle\')\nnexe_attr = ("Format:pickle", "Name:sorter", "Arg1:pickle", "Type:python")\nfor i in range(4):\n    if inputmnfst[index][20:-1] in nexe_attr:\n        retrieve_mnfst_field("#x-nexe-attr       ",inputmnfst[index][20:-1])\n    else:\n        raise Exception("inputmnfst[index][20:-1] not in (\'Format:pickle\', \'Name:sorter\', \'Arg1:pickle\', \'Type:python\')")\nretrieve_mnfst_field(\'input_mnfst        \')\nretrieve_mnfst_field(\'output             \')\nretrieve_mnfst_field(\'maxoutput          \',64*1048576, True)\nretrieve_mnfst_field(\'output_mnfst       \')\nretrieve_mnfst_field(\'maxmnfstline       \', 1024, True)\nretrieve_mnfst_field(\'maxmnfstlines      \', 128, True)\nretrieve_mnfst_field(\'timeout            \', 5, True)\nretrieve_mnfst_field(\'kill_timeout       \', 1, True)\nretrieve_mnfst_field(\'?zerovm_retcode    \', \'required\')\nretrieve_mnfst_field(\'?zerovm_status     \', \'required\')\nretrieve_mnfst_field(\'?etag              \', \'required\')\nretrieve_mnfst_field(\'?#retcode          \', \'optional\')\nretrieve_mnfst_field(\'?#status           \', \'optional\')    \nretrieve_mnfst_field(\'?#content-type     \', \'optional\')\n# todo handle meta-tags\n# retrieve_mnfst_field(\'?#x-data-attr     \', \'optional\')\nimport cPickle\ninf = file(mnfst.input, \'r\')\nouf = file(mnfst.output, \'w\')\nid = cPickle.load(inf)\nprint \'inputdata:\\n\' + str(id) + \'\\n\'\nod = cPickle.dumps(eval(file(mnfst.nexe, \'r\').read()))\nprint \'outputdata:\\n\' + str(od) + \'\\n\'\nprint(\'\\n\\n%s\\n\\n\'%(str(od)))\nouf.write(od)\nfrom hashlib import md5\netag = md5()\netag.update(od)\netag = etag.hexdigest()\ninf.close()\nouf.close()\noufm = file(mnfst.output_mnfst,\'w\')\noufm.write(  \n        \'zerovm_retcode     =%s\\n\'\n        \'zerovm_status      =%s\\n\'\n        \'etag               =%s\\n\'\n        \'#retcode           =%s\\n\'\n        \'#status            =%s\\n\'\n        \'#content-type      =%s\\n\'\n        \'#x-data-attr      =Message:Hi\\n\'\n        \'#x-data-attr      =Base:still 10\\n\'\n        \'#x-data-attr      =AnotherMessage:blahblahblah\\n\'\n         % (retcode, status, etag, retcode, status, \'text\x2Fplain\'))\noufm.close()\nsys.exit(retcode)')

        def set_zerovm_mock():
            (_prosrv, _acc1srv, _acc2srv, _con1srv,
             _con2srv, _obj1srv, _obj2srv) = _test_servers
            fd, zerovm_mock = mkstemp()
            os.write(fd, get_zerovm_mock())
            _obj1srv.zerovm_exename = ['python', zerovm_mock]
            _obj1srv.zerovm_nexe_xparams = ['ok.', '0']
            _obj2srv.zerovm_exename = ['python', zerovm_mock]
            _obj2srv.zerovm_nexe_xparams = ['ok.', '0']

        def get_random_numbers():
            import random
            _max_num = 10
            numlist = [i for i in range(_max_num)]
            for i in range(_max_num):
                randindex1 = random.randrange(_max_num)
                randindex2 = random.randrange(_max_num)
                numlist[randindex1], numlist[randindex2] =\
                numlist[randindex2], numlist[randindex1]
            return pickle.dumps(numlist, protocol=0)

        def create_container(prolis):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/c HTTP/1.1\r\nHost: localhost\r\n'
                     'Connection: close\r\nX-Storage-Token: t\r\n'
                     'Content-Length: 0\r\n'
                     '\r\n')
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 202'
            self.assertEquals(headers[:len(exp)], exp)

        def create_object(prolis):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/c/o HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Storage-Token: t\r\n'
                     'Content-Length: %s\r\n'
                     'Content-Type: application/octet-stream\r\n'
                     'X-Object-Meta-Message: Hello\r\n'
                     'X-Object-Meta-Base: 10\r\n'
                     'X-Object-Meta-Format: Pickle\r\n'
                     '\r\n%s' % (str(len(self._randomnumbers)),  self._randomnumbers))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 201'
            self.assertEquals(headers[:len(exp)], exp)

        self._randomnumbers = get_random_numbers()
        self._nexescript = ('sorted(id)')
        self._sortednumbers = '(lp1\nI0\naI1\naI2\naI3\naI4\naI5\naI6\naI7\naI8\naI9\na.'
        self._randomnumbers_etag = md5()
        self._randomnumbers_etag.update(self._randomnumbers)
        self._randomnumbers_etag = self._randomnumbers_etag.hexdigest()
        self._sortednumbers_etag = md5()
        self._sortednumbers_etag.update(self._sortednumbers)
        self._sortednumbers_etag = self._sortednumbers_etag.hexdigest()
        self._nexescript_etag = md5()
        self._nexescript_etag.update(self._nexescript)
        self._nexescript_etag = self._nexescript_etag.hexdigest()
        set_zerovm_mock()

        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis) = _test_sockets
        create_container(prolis)
        create_object(prolis)

    def zerovm_request(self):
        req = Request.blank('/v1/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'Content-Type': 'application/octet-stream',
                     'x-zerovm-execute': '1.0'})
        return req

    def test_QUERY_real_zerovm(self):

        raise SkipTest
        #This test will work only if ZeroVM is properly installed on the system
        #at /home/dazo/ZeroVM

        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets

        #create container
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 202'
        self.assertEquals(headers[:len(exp)], exp)

        # create data object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        file = open('/home/dazo/ZeroVM/samples/sort_mapping/input.data', 'rb')
        data = file.read()
        fd.write('PUT /v1/a/c/o HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Object-Meta-Message: Hello\r\n'
                 'X-Object-Meta-Base: 10\r\n'
                 'X-Object-Meta-Format: Pickle\r\n'
                 '\r\n' % str(len(data)))
        fd.write(data)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)

        # query
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        file = open('/home/dazo/ZeroVM/samples/sort_mapping/sort_uint_proper_'
                    'with_args.c_x86_64.nexe', 'rb')
        code = file.read()
        fd.write('QUERY /v1/a/c/o HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n' % str(len(code)))
        fd.write(code)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        file = open('/home/dazo/ZeroVM/samples/sort_mapping/output.data', 'rb')
        expres = file.read()
        self.assertEquals(res, expres)
        headerslines = headers.splitlines()
        headers = {}
        for line in headerslines:
            colon = line.find(":")
            headers[line[:colon].lower()] = line[colon+1:].strip()
        self.assertEquals(headers['content-length'], str(len(expres)))
        self.assertEquals(headers['content-type'], 'application/octet-stream')
        # TODOLE take care of etags passing, receiving and checking

    def test_QUERY_request_bytes_transferred_attr(self):
        with save_globals():
            proxyquery.http_connect =\
            fake_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxyquery.QueryController(self.proxy_app, 'a', 'c', 'o')
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'POST'},
                headers={'Content-Length': '10',
                         'x-zerovm-execute': '1.0'},
                body='1234567890')
            self.proxy_app.update_request(req)
            res = controller.zerovm_query(req)
            res.body
            self.assertEquals(res.status_int, 200)
            self.assert_(hasattr(req, 'bytes_transferred'))
            self.assertEquals(req.bytes_transferred, 10)
            self.assert_(hasattr(res, 'bytes_transferred'))
            self.assertEquals(res.bytes_transferred, 10)

    def test_QUERY_sort(self):
        self.setup_QUERY()
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('POST /v1/a/c/o HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'x-zerovm-execute: 1.0\r\n'
                 'etag: %s\r\n'
                 'X-Object-Meta-Type: python\r\n'
                 'X-Object-Meta-Name: sorter\r\n'
                 'X-Object-Meta-arg1: pickle\r\n'
                 'X-Object-Meta-format: pickle\r\n'
                 '\r\n%s' % (str(len(self._nexescript)),
                             self._nexescript_etag, self._nexescript))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        self.assertEquals(res, self._sortednumbers)

    def test_QUERY_sort_with_load_from(self):
        self.setup_QUERY()
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets
        # create code object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/co HTTP/1.1\r\n'
                 'Host: '
                 'localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Object-Meta-Format: pickle\r\n'
                 'X-Object-Meta-Name: sorter\r\n'
                 'X-Object-Meta-Arg1: pickle\r\n'
                 'X-Object-Meta-Type: python\r\n'
                 'etag: %s\r\n'
                 '\r\n%s' % (str(len(self._nexescript)),
                             self._nexescript_etag, self._nexescript))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        #run the query
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('POST /v1/a/c/o HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n'
                 'x-zerovm-execute: 1.0\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Load-From: c/co\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        self.assertEquals(res, self._sortednumbers)
        # Test invalid path inside "X-Load-From" http header.
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('POST /v1/a/c/o HTTP/1.1\r\n'
                 'Host: '
                 'localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'x-zerovm-execute: 1.0\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'etag: %s\r\n'
                 'X-Load-From: blah-blah-blah\r\n'
                 'X-Object-Meta-Type: python\r\n'
                 'X-Object-Meta-Name: sorter\r\n'
                 'X-Object-Meta-arg1: pickle\r\n'
                 'X-Object-Meta-format: pickle\r\n'
                 '\r\n%s' % (str(len(self._nexescript)),
                             self._nexescript_etag, self._nexescript))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)

    def test_QUERY_sort_with_store_to(self):
        self.setup_QUERY()
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('POST /v1/a/c/o HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'x-zerovm-execute: 1.0\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Store-To: c/o2\r\n'
                 'etag: %s\r\n'
                 'X-Object-Meta-Format: pickle\r\n'
                 'X-Object-Meta-Name: sorter\r\n'
                 'X-Object-Meta-Arg1: pickle\r\n'
                 'X-Object-Meta-Type: python\r\n'
                 '\r\n%s' % (str(len(self._nexescript)),
                             self._nexescript_etag, self._nexescript))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        self.assertEquals(res, '201 Created\n\n\n\n   ')
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/o2 HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'Content-Length: 0\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        self.assertEquals(res, self._sortednumbers)

        self.setup_QUERY()
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('POST /v1/a/c/o HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'x-zerovm-execute: 1.0\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Store-To: invalid'
                 'etag: %s\r\n'
                 'X-Object-Meta-Format: pickle\r\n'
                 'X-Object-Meta-Name: sorter\r\n'
                 'X-Object-Meta-Arg1: pickle\r\n'
                 'X-Object-Meta-Type: python\r\n'
                 '\r\n%s' % (str(len(self._nexescript)),
                             self._nexescript_etag, self._nexescript))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        self.assertEquals(res, 'X-Store-To header must be of the form<container name>/<object name>')


        self.setup_QUERY()
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('POST /v1/a/c/o HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'x-zerovm-execute: 1.0\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Store-To: invalid/invalid/invalid/o2\r\n'
                 'etag: %s\r\n'
                 'X-Object-Meta-Format: pickle\r\n'
                 'X-Object-Meta-Name: sorter\r\n'
                 'X-Object-Meta-Arg1: pickle\r\n'
                 'X-Object-Meta-Type: python\r\n'
                 '\r\n%s' % (str(len(self._nexescript)),
                             self._nexescript_etag, self._nexescript))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        self.assertEquals(res, '404 Not Found\n\nThe resource could not be found.\n\n   ')

    def test_QUERY_sort_with_load_from_and_store_to(self):
        self.setup_QUERY()
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets
        # create code object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/co HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: %s\r\nContent-Type: application/octet-'
                 'stream\r\n'
                 'etag: %s\r\n'
                 '\r\n%s' % (str(len(self._nexescript)),
                             self._nexescript_etag, self._nexescript))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        #run the query
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('QUERY /v1/a/c/o HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 0\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Object-Meta-Format: pickle\r\n'
                 'X-Object-Meta-Name: sorter\r\n'
                 'X-Object-Meta-Arg1: pickle\r\n'
                 'X-Object-Meta-Type: python\r\n'
                 'X-Load-From: c/co\r\n'
                 'X-Store-To: c/o2\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        self.assertEquals(res, '201 Created\n\n\n\n   ')
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/o2 HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'Content-Length: 0\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        res = fd.read()
        self.assertEquals(res, self._sortednumbers)

    def test_QUERY_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            proxy_server.http_connect =\
            fake_http_connect(200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            req = Request.blank('/a/c/o')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.QUERY(req)
        self.assert_(called[0])

    def test_QUERY_request_client_disconnect_attr(self):
        with save_globals():
            proxy_server.http_connect =\
            fake_http_connect(200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '10'},
                body='12345')
            self.app.update_request(req)
            res = controller.QUERY(req)
            self.assertEquals(req.bytes_transferred, 5)
            self.assert_(hasattr(req, 'client_disconnect'))
            self.assert_(req.client_disconnect)

    def test_QUERY_response_client_disconnect_attr(self):
        with save_globals():
            proxy_server.http_connect =\
            fake_http_connect(200, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            req = Request.blank('/a/c/o')
            self.app.update_request(req)
            orig_object_chunk_size = self.app.object_chunk_size
            try:
                self.app.object_chunk_size = 5
                res = controller.QUERY(req)
                ix = 0
                for v in res.app_iter:
                    ix += 1
                    if ix > 1:
                        break
                res.app_iter.close()
                self.assertEquals(res.bytes_transferred, 5)
                self.assert_(hasattr(res, 'client_disconnect'))
                self.assert_(res.client_disconnect)
            finally:
                self.app.object_chunk_size = orig_object_chunk_size

    def test_QUERY_invalid_path(self):
        self.setup_QUERY()
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets
        # create code object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/co HTTP/1.1\r\n'
                 'Host: '
                 'localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Object-Meta-Format: pickle\r\n'
                 'X-Object-Meta-Name: sorter\r\n'
                 'X-Object-Meta-Arg1: pickle\r\n'
                 'X-Object-Meta-Type: python\r\n'
                 'etag: %s\r\n'
                 '\r\n%s' % (str(len(self._nexescript)),
                             self._nexescript_etag, self._nexescript))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        #run the query
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('QUERY /v1/invalid/invalid/invalid HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'X-Load-From: c/co\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEquals(headers[:len(exp)], exp)

    def test_QUERY_chunked_lobjects(self):
        # Create a container for our segmented/manifest object testing
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) =\
        _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/segmented2 HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Create the object segments
        segment_etags = []
        for segment in xrange(5):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/segmented2/name/%s HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Storage-Token: '
                     't\r\nContent-Length: 5\r\n\r\n1234 ' % str(segment))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 201'
            self.assertEquals(headers[:len(exp)], exp)
            segment_etags.append(md5('1234 ').hexdigest())
            # Create the object manifest file
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/segmented2/name HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 0\r\nX-Object-Manifest: '
                 'segmented2/name/\r\nContent-Type: text/jibberish\r\n'
                 'Foo: barbaz\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Ensure retrieving the manifest file gets the whole object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/segmented2/name HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: '
                 't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('X-Object-Manifest: segmented2/name/' in headers)
        self.assert_('Content-Type: text/jibberish' in headers)
        self.assert_('Foo: barbaz' in headers)
        expected_etag = md5(''.join(segment_etags)).hexdigest()
        self.assert_('Etag: "%s"' % expected_etag in headers)
        body = fd.read()
        self.assertEquals(body, '1234 1234 1234 1234 1234 ')
        # Do it again but exceeding the container listing limit
        proxy_server.CONTAINER_LISTING_LIMIT = 2
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/segmented2/name HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: '
                 't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('X-Object-Manifest: segmented2/name/' in headers)
        self.assert_('Content-Type: text/jibberish' in headers)
        body = fd.read()
        # A bit fragile of a test; as it makes the assumption that all
        # will be sent in a single chunk.
        self.assertEquals(body,
            '19\r\n1234 1234 1234 1234 1234 \r\n0\r\n\r\n')
        # Make a copy of the manifested object, which should
        # error since the number of segments exceeds
        # CONTAINER_LISTING_LIMIT.
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('QUERY /v1/a/segmented2/copy HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: '
                 't\r\nX-Load-From: segmented2/name\r\nContent-Length: '
                 '0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 413'
        self.assertEquals(headers[:len(exp)], exp)
        body = fd.read()

    def test_QUERY_chunked(self):
        with save_globals():
            proxy_server.http_connect =\
            fake_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'QUERY'},
                headers={'Content-Length': '10',
                         'transfer-encoding': 'chunked'},
                body='1234567890')
            self.app.update_request(req)
            res = controller.QUERY(req)
            res.body
            self.assertEquals(res.status_int, 200)

    def test_QUERY_zerovm_maxnexe(self):
        with save_globals():
            proxy_server.http_connect =\
            fake_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'QUERY'},
                headers={'Content-Length': '10'},
                body='1234567890')
            self.app.update_request(req)
            controller.zerovm_maxnexe = 0
            res = controller.QUERY(req)
            res.body
            self.assertEquals(res.status_int, 413)

    def test_QUERY_connection_getexpect_timeout(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'

                def getresponse(self):
                    return self

                def read(self, amt=None):
                    return ''

                def getheader(self, name):
                    return ''

                def getexpect(self):
                    sleep(0.5)

            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                status = code_iter.next()
                if status == -1:
                    raise HTTPException()
                return FakeConn(status)

            return connect

        with save_globals():
            proxy_server.http_connect =\
            mock_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            controller.app.node_timeout = 0.1
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'QUERY'},
                headers={'Content-Length': '10'},
                body='1234567890')
            self.app.update_request(req)
            try:
                controller.QUERY(req)
            except Exception, msg:
                self.assert_('Cannot find suitable node to execute code on',
                    msg)
            else:
                raise Exception

    def test_QUERY_connection_send_timeout(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'

                def getresponse(self):
                    return self

                def read(self, amt=None):
                    return ''

                def getheader(self, name):
                    return ''

                def getexpect(self):
                    return FakeConn(100)

                def send(self, param):
                    sleep(0.5)

            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                status = code_iter.next()
                if status == -1:
                    raise HTTPException()
                return FakeConn(status)

            return connect

        with save_globals():
            proxy_server.http_connect =\
            mock_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            controller.app.node_timeout = 0.1
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'QUERY'},
                headers={'Content-Length': '10',
                         'Transfer-Encoding': 'chunked'},
                body='1234567890')
            self.app.update_request(req)
            resp = controller.QUERY(req)
            self.assertEquals(resp.status, '408 Request Timeout')

    def test_QUERY_connection_send_fail(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'

                def getresponse(self):
                    return self

                def read(self, amt=None):
                    return ''

                def getheader(self, name):
                    return ''

                def getexpect(self):
                    return FakeConn(100)

            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                status = code_iter.next()
                if status == -1:
                    raise HTTPException()
                return FakeConn(status)

            return connect

        with save_globals():
            proxy_server.http_connect =\
            mock_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'QUERY'},
                headers={'Content-Length': '10'},
                body='1234567890')
            self.app.update_request(req)
            res = controller.QUERY(req)
            res.body
            self.assertEquals(res.status_int, 499)

    def test_QUERY_file_iter_fail(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'

                def getresponse(self):
                    return self

                def read(self, amt=None):
                    sleep(0.5)

                def getheader(self, name):
                    return ''

                def getexpect(self):
                    return FakeConn(100)

                def send(self, param):
                    return ''

                def getheaders(self):
                    return ''

            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                status = code_iter.next()
                if status == -1:
                    raise HTTPException()
                return FakeConn(status)

            return connect

        with save_globals():
            proxy_server.http_connect =\
            mock_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            controller.app.node_timeout = .002
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'QUERY'},
                headers={'Content-Length': '10'},
                body='1234567890')
            self.app.update_request(req)
            res = controller.QUERY(req)
            from swift.common.exceptions import ChunkReadTimeout
            try:
                res.body
            except ChunkReadTimeout:
                pass
            else:
                raise Exception

    def test_QUERY_connection_getresponse_timeout(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'

                def getresponse(self):
                    sleep(0.5)

                def read(self, amt=None):
                    return ''

                def getheader(self, name):
                    return ''

                def getexpect(self):
                    return FakeConn(100)

                def send(self, param):
                    return ''

            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                status = code_iter.next()
                if status == -1:
                    raise HTTPException()
                return FakeConn(status)

            return connect

        with save_globals():
            proxy_server.http_connect =\
            mock_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            controller.app.node_timeout = 0.1
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'QUERY'},
                headers={'Content-Length': '10',
                         'Transfer-Encoding': 'chunked'},
                body='1234567890')
            self.app.update_request(req)
            resp = controller.QUERY(req)
            self.assertEquals(resp.status, '499 Client Disconnect')

    def test_QUERY_connection_getresponse2_timeout(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'

                def getresponse(self):
                    self.status = 201
                    return self

                def read(self, amt=None):
                    return ''

                def getheader(self, name):
                    return ''

                def getexpect(self):
                    return FakeConn(100)

                def send(self, param):
                    return ''

            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                status = code_iter.next()
                if status == -1:
                    raise HTTPException()
                return FakeConn(status)

            return connect

        with save_globals():
            proxy_server.http_connect =\
            mock_http_connect(200, 200, 201, 201, 201, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            controller.app.node_timeout = 0.1
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'QUERY'},
                headers={'Content-Length': '10',
                         'Transfer-Encoding': 'chunked'},
                body='1234567890')
            self.app.update_request(req)
            try:
                controller.QUERY(req)
            except Exception, msg:
                self.assert_('Error querying object server', msg)
            else:
                raise Exception
    '''
    def test_QUERY_connect_exceptions(self):

    def mock_http_connect(*code_iter, **kwargs):

    class FakeConn(object):

        def __init__(self, status):
            self.status = status
            self.reason = 'Fake'

        def getresponse(self):
            return self

        def read(self, amt=None):
            return ''

        def getheader(self, name):
            return ''

        def getexpect(self):
            if self.status == -2:
                raise HTTPException()
            if self.status == -3:
                return FakeConn(507)
            return FakeConn(100)

    code_iter = iter(code_iter)

    def connect(*args, **ckwargs):
        status = code_iter.next()
        if status == -1:
            raise HTTPException()
        return FakeConn(status)

    return connect

    with save_globals():
    controller = proxy_server.ObjectController(self.app, 'account',
        'container', 'object')

    def test_status_map(statuses, expected):
        proxy_server.http_connect = mock_http_connect(*statuses)
        self.app.memcache.store = {}
        req = Request.blank('/a/c/o.jpg', {})
        req.content_length = 0
        self.app.update_request(req)
        res = controller.QUERY(req)
        expected = str(expected)
        self.assertEquals(res.status[:len(expected)], expected)
    test_status_map((200, 200, 201, 201, -1), 201)
    test_status_map((200, 200, 201, 201, -2), 201)  # expect timeout
    test_status_map((200, 200, 201, 201, -3), 201)  # error limited
    test_status_map((200, 200, 201, -1, -1), 503)
    test_status_map((200, 200, 503, 503, -1), 503)
    def test_QUERY_send_exceptions(self):

    def mock_http_connect(*code_iter, **kwargs):

    class FakeConn(object):

        def __init__(self, status):
            self.status = status
            self.reason = 'Fake'
            self.host = '1.2.3.4'
            self.port = 1024
            self.etag = md5()

        def getresponse(self):
            self.etag = self.etag.hexdigest()
            self.headers = {
                'etag': self.etag,
            }
            return self

        def read(self, amt=None):
            return ''

        def send(self, amt=None):
            if self.status == -1:
                raise HTTPException()
            else:
                self.etag.update(amt)

        def getheader(self, name):
            return self.headers.get(name, '')

        def getexpect(self):
            return FakeConn(100)
    code_iter = iter(code_iter)

    def connect(*args, **ckwargs):
        return FakeConn(code_iter.next())
    return connect
    with save_globals():
    controller = proxy_server.ObjectController(self.app, 'account',
        'container', 'object')

    def test_status_map(statuses, expected):
        self.app.memcache.store = {}
        proxy_server.http_connect = mock_http_connect(*statuses)
        req = Request.blank('/a/c/o.jpg',
            environ={'REQUEST_METHOD': 'QUERY'}, body='some data')
        self.app.update_request(req)
        res = controller.QUERY(req)
        expected = str(expected)
        self.assertEquals(res.status[:len(expected)], expected)
    test_status_map((200, 200, 201, -1, 201), 201)
    test_status_map((200, 200, 201, -1, -1), 503)
    test_status_map((200, 200, 503, 503, -1), 503)
    def test_QUERY_max_size(self):
    with save_globals():
    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    controller = proxy_server.ObjectController(self.app, 'account',
        'container', 'object')
    req = Request.blank('/a/c/o', {}, headers={
        'Content-Length': str(MAX_FILE_SIZE + 1),
        'Content-Type': 'foo/bar'})
    self.app.update_request(req)
    res = controller.QUERY(req)
    self.assertEquals(res.status_int, 413)
    def test_QUERY_getresponse_exceptions(self):

    def mock_http_connect(*code_iter, **kwargs):

    class FakeConn(object):

        def __init__(self, status):
            self.status = status
            self.reason = 'Fake'
            self.host = '1.2.3.4'
            self.port = 1024

        def getresponse(self):
            if self.status == -1:
                raise HTTPException()
            return self

        def read(self, amt=None):
            return ''

        def send(self, amt=None):
            pass

        def getheader(self, name):
            return ''

        def getexpect(self):
            return FakeConn(100)
    code_iter = iter(code_iter)

    def connect(*args, **ckwargs):
        return FakeConn(code_iter.next())
    return connect
    with save_globals():
    controller = proxy_server.ObjectController(self.app, 'account',
        'container', 'object')

    def test_status_map(statuses, expected):
        self.app.memcache.store = {}
        proxy_server.http_connect = mock_http_connect(*statuses)
        req = Request.blank('/a/c/o.jpg', {})
        req.content_length = 0
        self.app.update_request(req)
        res = controller.QUERY(req)
        expected = str(expected)
        self.assertEquals(res.status[:len(str(expected))],
                          str(expected))
    test_status_map((200, 200, 201, 201, -1), 201)
    test_status_map((200, 200, 201, -1, -1), 503)
    test_status_map((200, 200, 503, 503, -1), 503)
    def test_QUERY_client_timeout(self):
    with save_globals():
    self.app.account_ring.get_nodes('account')
    for dev in self.app.account_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    self.app.container_ring.get_nodes('account')
    for dev in self.app.container_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    self.app.object_ring.get_nodes('account')
    for dev in self.app.object_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1

    class SlowBody():

        def __init__(self):
            self.sent = 0

        def read(self, size=-1):
            if self.sent < 4:
                sleep(0.1)
                self.sent += 1
                return ' '
            return ''

    req = Request.blank('/a/c/o',
        environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
        headers={'Content-Length': '4', 'Content-Type': 'text/plain'})
    self.app.update_request(req)
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 201, 201, 201)
        #                 acct cont obj  obj  obj
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    self.app.client_timeout = 0.1
    req = Request.blank('/a/c/o',
        environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
        headers={'Content-Length': '4', 'Content-Type': 'text/plain'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(201, 201, 201)
        #                 obj  obj  obj
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 408)
    def test_QUERY_client_disconnect(self):
    with save_globals():
    self.app.account_ring.get_nodes('account')
    for dev in self.app.account_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    self.app.container_ring.get_nodes('account')
    for dev in self.app.container_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    self.app.object_ring.get_nodes('account')
    for dev in self.app.object_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1

    class SlowBody():

        def __init__(self):
            self.sent = 0

        def read(self, size=-1):
            raise Exception('Disconnected')

    req = Request.blank('/a/c/o',
        environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
        headers={'Content-Length': '4', 'Content-Type': 'text/plain'})
    self.app.update_request(req)
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 201, 201, 201)
        #                 acct cont obj  obj  obj
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 499)
    def test_QUERY_node_read_timeout(self):
    with save_globals():
    self.app.account_ring.get_nodes('account')
    for dev in self.app.account_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    self.app.container_ring.get_nodes('account')
    for dev in self.app.container_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    self.app.object_ring.get_nodes('account')
    for dev in self.app.object_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'GET'})
    self.app.update_request(req)
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, slow=True)
    req.sent_size = 0
    resp = controller.GET(req)
    got_exc = False
    try:
        resp.body
    except proxy_server.ChunkReadTimeout:
        got_exc = True
    self.assert_(not got_exc)
    self.app.node_timeout = 0.1
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, slow=True)
    resp = controller.GET(req)
    got_exc = False
    try:
        resp.body
    except proxy_server.ChunkReadTimeout:
        got_exc = True
    self.assert_(got_exc)
    def test_QUERY_node_write_timeout(self):
    with save_globals():
    self.app.account_ring.get_nodes('account')
    for dev in self.app.account_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    self.app.container_ring.get_nodes('account')
    for dev in self.app.container_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    self.app.object_ring.get_nodes('account')
    for dev in self.app.object_ring.devs.values():
        dev['ip'] = '127.0.0.1'
        dev['port'] = 1
    req = Request.blank('/a/c/o',
        environ={'REQUEST_METHOD': 'PUT'},
        headers={'Content-Length': '4', 'Content-Type': 'text/plain'},
        body='    ')
    self.app.update_request(req)
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 201, 201, 201, slow=True)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    self.app.node_timeout = 0.1
    proxy_server.http_connect = \
        fake_http_connect(201, 201, 201, slow=True)
    req = Request.blank('/a/c/o',
        environ={'REQUEST_METHOD': 'PUT'},
        headers={'Content-Length': '4', 'Content-Type': 'text/plain'},
        body='    ')
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 503)
    def test_QUERY_iter_nodes(self):
    with save_globals():
    try:
        self.app.object_ring.max_more_nodes = 2
        controller = proxy_server.ObjectController(self.app, 'account',
                        'container', 'object')
        partition, nodes = self.app.object_ring.get_nodes('account',
                            'container', 'object')
        collected_nodes = []
        for node in controller.iter_nodes(partition, nodes,
                                          self.app.object_ring):
            collected_nodes.append(node)
        self.assertEquals(len(collected_nodes), 5)

        self.app.object_ring.max_more_nodes = 20
        controller = proxy_server.ObjectController(self.app, 'account',
                        'container', 'object')
        partition, nodes = self.app.object_ring.get_nodes('account',
                            'container', 'object')
        collected_nodes = []
        for node in controller.iter_nodes(partition, nodes,
                                          self.app.object_ring):
            collected_nodes.append(node)
        self.assertEquals(len(collected_nodes), 9)
    finally:
        self.app.object_ring.max_more_nodes = 0
    def test_QUERY_proxy_passes_content_type(self):
    with save_globals():
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'GET'})
    self.app.update_request(req)
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    proxy_server.http_connect = fake_http_connect(200, 200, 200)
    resp = controller.GET(req)
    self.assertEquals(resp.status_int, 200)
    self.assertEquals(resp.content_type, 'x-application/test')
    proxy_server.http_connect = fake_http_connect(200, 200, 200)
    resp = controller.GET(req)
    self.assertEquals(resp.status_int, 200)
    self.assertEquals(resp.content_length, 0)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, slow=True)
    resp = controller.GET(req)
    self.assertEquals(resp.status_int, 200)
    self.assertEquals(resp.content_length, 4)
    def test_QUERY_proxy_passes_content_length(self):
    with save_globals():
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'HEAD'})
    self.app.update_request(req)
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    proxy_server.http_connect = fake_http_connect(200, 200, 200)
    resp = controller.HEAD(req)
    self.assertEquals(resp.status_int, 200)
    self.assertEquals(resp.content_length, 0)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, slow=True)
    resp = controller.HEAD(req)
    self.assertEquals(resp.status_int, 200)
    self.assertEquals(resp.content_length, 4)
    def test_QUERY_error_limiting(self):
    with save_globals():
    proxy_server.shuffle = lambda l: None
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    self.assert_status_map(controller.HEAD, (503, 200, 200), 200)
    self.assertEquals(controller.app.object_ring.devs[0]['errors'], 2)
    self.assert_('last_error' in controller.app.object_ring.devs[0])
    for _junk in xrange(self.app.error_suppression_limit):
        self.assert_status_map(controller.HEAD, (503, 503, 503), 503)
    self.assertEquals(controller.app.object_ring.devs[0]['errors'],
                      self.app.error_suppression_limit + 1)
    self.assert_status_map(controller.HEAD, (200, 200, 200), 503)
    self.assert_('last_error' in controller.app.object_ring.devs[0])
    self.assert_status_map(controller.PUT, (200, 201, 201, 201), 503)
    self.assert_status_map(controller.POST,
                           (200, 200, 200, 200, 202, 202, 202), 503)
    self.assert_status_map(controller.DELETE,
                           (200, 204, 204, 204), 503)
    self.app.error_suppression_interval = -300
    self.assert_status_map(controller.HEAD, (200, 200, 200), 200)
    self.assertRaises(BaseException,
        self.assert_status_map, controller.DELETE,
        (200, 204, 204, 204), 503, raise_exc=True)
    def test_QUERY_acc_or_con_missing_returns_404(self):
    with save_globals():
    self.app.memcache = FakeMemcacheReturnsNone()
    for dev in self.app.account_ring.devs.values():
        del dev['errors']
        del dev['last_error']
    for dev in self.app.container_ring.devs.values():
        del dev['errors']
        del dev['last_error']
    controller = proxy_server.ObjectController(self.app, 'account',
                                             'container', 'object')
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 200)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'DELETE'})
    self.app.update_request(req)
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 200)

    proxy_server.http_connect = \
        fake_http_connect(404, 404, 404)
        #                 acct acct acct
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 404)

    proxy_server.http_connect = \
        fake_http_connect(503, 404, 404)
        #                 acct acct acct
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 404)

    proxy_server.http_connect = \
        fake_http_connect(503, 503, 404)
        #                 acct acct acct
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 404)

    proxy_server.http_connect = \
        fake_http_connect(503, 503, 503)
        #                 acct acct acct
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 404)

    proxy_server.http_connect = \
        fake_http_connect(200, 200, 204, 204, 204)
        #                 acct cont obj  obj  obj
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 204)

    proxy_server.http_connect = \
        fake_http_connect(200, 404, 404, 404)
        #                 acct cont cont cont
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 404)

    proxy_server.http_connect = \
        fake_http_connect(200, 503, 503, 503)
        #                 acct cont cont cont
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 404)

    for dev in self.app.account_ring.devs.values():
        dev['errors'] = self.app.error_suppression_limit + 1
        dev['last_error'] = time()
    proxy_server.http_connect = \
        fake_http_connect(200)
        #                 acct [isn't actually called since everything
        #                       is error limited]
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 404)

    for dev in self.app.account_ring.devs.values():
        dev['errors'] = 0
    for dev in self.app.container_ring.devs.values():
        dev['errors'] = self.app.error_suppression_limit + 1
        dev['last_error'] = time()
    proxy_server.http_connect = \
        fake_http_connect(200, 200)
        #                 acct cont [isn't actually called since
        #                            everything is error limited]
    resp = getattr(controller, 'DELETE')(req)
    self.assertEquals(resp.status_int, 404)
    def test_QUERY_PUT_POST_requires_container_exist(self):
    with save_globals():
    self.app.object_post_as_copy = False
    self.app.memcache = FakeMemcacheReturnsNone()
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')

    proxy_server.http_connect = \
        fake_http_connect(200, 404, 404, 404, 200, 200, 200)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'})
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 404)

    proxy_server.http_connect = \
        fake_http_connect(200, 404, 404, 404, 200, 200)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'POST'},
                        headers={'Content-Type': 'text/plain'})
    self.app.update_request(req)
    resp = controller.POST(req)
    self.assertEquals(resp.status_int, 404)
    def test_QUERY_PUT_POST_as_copy_requires_container_exist(self):
    with save_globals():
    self.app.memcache = FakeMemcacheReturnsNone()
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    proxy_server.http_connect = \
        fake_http_connect(200, 404, 404, 404, 200, 200, 200)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'})
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 404)

    proxy_server.http_connect = \
        fake_http_connect(200, 404, 404, 404, 200, 200, 200, 200, 200,
                          200)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'POST'},
                        headers={'Content-Type': 'text/plain'})
    self.app.update_request(req)
    resp = controller.POST(req)
    self.assertEquals(resp.status_int, 404)
    def test_QUERY_bad_metadata(self):
    with save_globals():
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 201, 201, 201)
        #                 acct cont obj  obj  obj
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0'})
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)

    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
        headers={'Content-Length': '0',
                 'X-Object-Meta-' + ('a' *
                    MAX_META_NAME_LENGTH): 'v'})
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
        headers={'Content-Length': '0',
                 'X-Object-Meta-' + ('a' *
                    (MAX_META_NAME_LENGTH + 1)): 'v'})
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 400)

    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
        headers={'Content-Length': '0',
                 'X-Object-Meta-Too-Long': 'a' *
                    MAX_META_VALUE_LENGTH})
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
        headers={'Content-Length': '0',
                 'X-Object-Meta-Too-Long': 'a' *
                    (MAX_META_VALUE_LENGTH + 1)})
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 400)

    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    headers = {'Content-Length': '0'}
    for x in xrange(MAX_META_COUNT):
        headers['X-Object-Meta-%d' % x] = 'v'
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers=headers)
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    headers = {'Content-Length': '0'}
    for x in xrange(MAX_META_COUNT + 1):
        headers['X-Object-Meta-%d' % x] = 'v'
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers=headers)
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 400)

    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    headers = {'Content-Length': '0'}
    header_value = 'a' * MAX_META_VALUE_LENGTH
    size = 0
    x = 0
    while size < MAX_META_OVERALL_SIZE - 4 - \
            MAX_META_VALUE_LENGTH:
        size += 4 + MAX_META_VALUE_LENGTH
        headers['X-Object-Meta-%04d' % x] = header_value
        x += 1
    if MAX_META_OVERALL_SIZE - size > 1:
        headers['X-Object-Meta-a'] = \
            'a' * (MAX_META_OVERALL_SIZE - size - 1)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers=headers)
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    proxy_server.http_connect = fake_http_connect(201, 201, 201)
    headers['X-Object-Meta-a'] = \
        'a' * (MAX_META_OVERALL_SIZE - size)
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers=headers)
    self.app.update_request(req)
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 400)
    def test_QUERY_copy_from(self):
    with save_globals():
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    # initial source object PUT
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 201, 201, 201)
        #                 acct cont obj  obj  obj
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)

    # basic copy
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': 'c/o'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
            201)
        #                 acct cont acct cont objc objc objc obj  obj
        #   obj
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    self.assertEquals(resp.headers['x-copied-from'], 'c/o')

    # non-zero content length
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '5',
                                  'X-Copy-From': 'c/o'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 200, 200)
        #                 acct cont acct cont objc objc objc
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 400)

    # extra source path parsing
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': 'c/o/o2'})
    req.account = 'a'
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
            201)
        #                 acct cont acct cont objc objc objc obj  obj
        #   obj
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

    # space in soure path
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': 'c/o%20o2'})
    req.account = 'a'
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
            201)
        #                 acct cont acct cont objc objc objc obj  obj
        #   obj
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    self.assertEquals(resp.headers['x-copied-from'], 'c/o%20o2')

    # repeat tests with leading /
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': '/c/o'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
            201)
        #                 acct cont acct cont objc objc objc obj  obj
        #   obj
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    self.assertEquals(resp.headers['x-copied-from'], 'c/o')

    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': '/c/o/o2'})
    req.account = 'a'
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
            201)
        #                 acct cont acct cont objc objc objc obj  obj
        #   obj
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

    # negative tests

    # invalid x-copy-from path
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': '/c'})
    self.app.update_request(req)
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int // 100, 4)  # client error

    # server error
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': '/c/o'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 503, 503, 503)
        #                 acct cont objc objc objc
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 503)

    # not found
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': '/c/o'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 404, 404, 404)
        #                 acct cont objc objc objc
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 404)

    # some missing containers
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': '/c/o'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 404, 404, 200, 201, 201, 201)
        #                 acct cont objc objc objc obj  obj  obj
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)

    # test object meta data
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0',
                                  'X-Copy-From': '/c/o',
                                  'X-Object-Meta-Ours': 'okay'})
    self.app.update_request(req)
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 201, 201, 201)
        #                 acct cont objc objc objc obj  obj  obj
    self.app.memcache.store = {}
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int, 201)
    self.assertEquals(resp.headers.get('x-object-meta-test'),
                      'testing')
    self.assertEquals(resp.headers.get('x-object-meta-ours'), 'okay')
    def test_QUERY_client_ip_logging(self):
    # test that the client ip field in the log gets populated with the
    # ip instead of being blank
    (prosrv, acc1srv, acc2srv, con2srv, con2srv, obj1srv, obj2srv) = \
        _test_servers
    (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
         _test_sockets

    class Logger(object):

    def info(self, msg):
        self.msg = msg

    orig_logger, orig_access_logger = prosrv.logger, prosrv.access_logger
    prosrv.logger = prosrv.access_logger = Logger()
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    fd.write(
    'GET /v1/a?format=json HTTP/1.1\r\nHost: localhost\r\n'
    'Connection: close\r\nX-Auth-Token: t\r\n'
    'Content-Length: 0\r\n'
    '\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = 'HTTP/1.1 200'
    self.assertEquals(headers[:len(exp)], exp)
    exp = '127.0.0.1 127.0.0.1'
    self.assert_(exp in prosrv.logger.msg)
    def test_QUERY_mismatched_etags(self):
    with save_globals():
    # no etag supplied, object servers return success w/ diff values
    controller = proxy_server.ObjectController(self.app, 'account',
                                               'container', 'object')
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'Content-Length': '0'})
    self.app.update_request(req)
    proxy_server.http_connect = fake_http_connect(200, 201, 201, 201,
        etags=[None,
               '68b329da9893e34099c7d8ad5cb9c940',
               '68b329da9893e34099c7d8ad5cb9c940',
               '68b329da9893e34099c7d8ad5cb9c941'])
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int // 100, 5)  # server error

    # req supplies etag, object servers return 422 - mismatch
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={
                            'Content-Length': '0',
                            'ETag': '68b329da9893e34099c7d8ad5cb9c940',
                        })
    self.app.update_request(req)
    proxy_server.http_connect = fake_http_connect(200, 422, 422, 503,
        etags=['68b329da9893e34099c7d8ad5cb9c940',
               '68b329da9893e34099c7d8ad5cb9c941',
               None,
               None])
    resp = controller.PUT(req)
    self.assertEquals(resp.status_int // 100, 4)  # client error
    def test_QUERY_copy_zero_bytes_transferred_attr(self):
    with save_globals():
    proxy_server.http_connect = \
        fake_http_connect(200, 200, 200, 200, 200, 201, 201, 201,
                          body='1234567890')
    controller = proxy_server.ObjectController(self.app, 'account',
                    'container', 'object')
    req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'X-Copy-From': 'c/o2',
                                 'Content-Length': '0'})
    self.app.update_request(req)
    res = controller.PUT(req)
    self.assert_(hasattr(req, 'bytes_transferred'))
    self.assertEquals(req.bytes_transferred, 0)
    def test_QUERY_response_bytes_transferred_attr(self):
    with save_globals():
    proxy_server.http_connect = \
        fake_http_connect(200, body='1234567890')
    controller = proxy_server.ObjectController(self.app, 'account',
                    'container', 'object')
    req = Request.blank('/a/c/o')
    self.app.update_request(req)
    res = controller.GET(req)
    res.body
    self.assert_(hasattr(res, 'bytes_transferred'))
    self.assertEquals(res.bytes_transferred, 10)
    '''