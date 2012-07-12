import unittest
import os
import random
import cPickle as pickle
from time import time, sleep
from webob import Request
from hashlib import md5
from tempfile import mkstemp, mkdtemp
from shutil import rmtree
import math

from swift.common import utils
from swift.common.middleware import objectquery
from swift.common.utils import mkdirs, normalize_timestamp
from swift.obj.server import ObjectController


class FakeApp(ObjectController):
    def __init__(self, conf):
        ObjectController.__init__(self, conf)
        self.bytes_per_sync = 1


class TestObjectQuery(unittest.TestCase):
    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        self.testdir =\
        os.path.join(mkdtemp(), 'tmp_test_object_server_ObjectController')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.obj_controller = FakeApp(conf)
        self.app = objectquery.ObjectQueryMiddleware(self.obj_controller, conf)

    def tearDown(self):
        """ Tear down for testing swift.object_server.ObjectController """
        rmtree(os.path.dirname(self.testdir))

    def setup_zerovm_query(self, mock=None, with_headers=False):
        def set_zerovm_mock():
            # ensure that python executable is used
            fd, zerovm_mock = mkstemp()
            if mock:
                os.write(fd, mock)
            else:
                os.write(fd, self.get_zerovm_mock())
            self.app.zerovm_exename = ['python', zerovm_mock]
            self.app.zerovm_nexe_xparams = ['ok.', '0']

        def create_random_numbers(max_num):
            numlist = [i for i in range(max_num)]
            for i in range(max_num):
                randindex1 = random.randrange(max_num)
                randindex2 = random.randrange(max_num)
                numlist[randindex1], numlist[randindex2] =\
                numlist[randindex2], numlist[randindex1]
            return pickle.dumps(numlist, protocol=0)

        def create_object(body):
            timestamp = normalize_timestamp(time())
            headers = {'X-Timestamp': timestamp,
                       'Content-Type': 'application/octet-stream'}

            if with_headers:
                headers['X-Object-Meta-Message'] = 'Hello'
                headers['X-Object-Meta-Base'] = '10'
                headers['X-Object-Meta-Format'] = 'Pickle'
            req = Request.blank('/sda1/p/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'}, headers=headers)
            req.body = body
            resp = req.get_response(self.app)
            self.assertEquals(resp.status_int, 201)

        set_zerovm_mock()
        randomnumbers = create_random_numbers(10)
        create_object(randomnumbers)
        self._nexescript = 'sorted(id)'
        self._sortednumbers = '(lp1\nI0\naI1\naI2\naI3\naI4\naI5\naI6\naI7\naI8\naI9\na.'
        self._randomnumbers_etag = md5()
        self._randomnumbers_etag.update(randomnumbers)
        self._randomnumbers_etag = self._randomnumbers_etag.hexdigest()
        self._sortednumbers_etag = md5()
        self._sortednumbers_etag.update(self._sortednumbers)
        self._sortednumbers_etag = self._sortednumbers_etag.hexdigest()
        self._nexescript_etag = md5()
        self._nexescript_etag.update(self._nexescript)
        self._nexescript_etag = self._nexescript_etag.hexdigest()

    def get_zerovm_mock(self):
        return (
            'import signal\nimport sys\nfrom eventlet import sleep\nfrom sys import argv \nif len(argv) \x3C 2 or len(argv) \x3E 5:\n    raise Exception(\'Incorrect number of arguments\')\nif argv[1] != \'-Y2\':\n    raise Exception(\'Invalid first argument: %s\' % sys.argv[1])\nif argv[2][:2] != \'-M\':\n    raise Exception(\'Invalid second argument: %s\' % sys.argv[2])\nmanifest = argv[2][2:]\ninputmnfst = file(manifest, \'r\').readlines()\nclass Mnfst:\n    pass\nmnfst = Mnfst()\nindex = 0\nstatus = \'ok.\' if len(argv) \x3C 3 else argv[3]\nprint \'param status:\' + str(status) + \'\\n\'\nretcode = 0 if len(argv) \x3C 4 else argv[4]\nprint \'param retcode:\' + str(retcode) + \'\\n\'\ndef retrieve_mnfst_field(n, vv = None, isint=False, rgx=None):\n    global index                  \n    if len(inputmnfst) \x3C index:\n        raise Exception(\'missing  %s\' % n)\n    if not inputmnfst[index][0:19] == n:\n        raise Exception(\'expecting %s got %s\' \n                        % (n, inputmnfst[index][0:19]))       \n    if not inputmnfst[index][19] == \'=\':\n        raise Exception(\'missing \\\'=\\\' at %s got %s\' \n                        % (n, inputmnfst[index][19]))       \n    v = inputmnfst[index][20:-1]\n    if isint:\n        v = int(v)\n    if rgx:\n        import re\n        if not re.match(rgx,v):\n            raise Exception(\'mnfst: %s does not match %s\' % n, rgx)\n    if n[0] == \'?\':\n        n = n[1:]\n    if vv and vv != v:\n        raise Exception(\'for %s expected %s got %s\' % (n, vv, v))\n    index += 1\n    setattr(mnfst, n.strip(), v)\nretrieve_mnfst_field(\'version            \',\'11nov2011\')\nretrieve_mnfst_field(\'zerovm             \')\nretrieve_mnfst_field(\'nexe               \')\nretrieve_mnfst_field(\'maxnexe            \', 256*1048576, True)\nretrieve_mnfst_field(\'input              \')\nretrieve_mnfst_field(\'maxinput           \', 256*1048576, True)\nretrieve_mnfst_field(\'#etag              \')\nretrieve_mnfst_field(\'#content-type      \')\nretrieve_mnfst_field(\'#x-timestamp       \')\nretrieve_mnfst_field(\'input_mnfst        \')\nretrieve_mnfst_field(\'output             \')\nretrieve_mnfst_field(\'maxoutput          \',64*1048576, True)\nretrieve_mnfst_field(\'output_mnfst       \')\nretrieve_mnfst_field(\'maxmnfstline       \', 1024, True)\nretrieve_mnfst_field(\'maxmnfstlines      \', 128, True)\nretrieve_mnfst_field(\'timeout            \', isint=True)\nretrieve_mnfst_field(\'kill_timeout       \', isint=True)\nretrieve_mnfst_field(\'?zerovm_retcode    \', \'required\')\nretrieve_mnfst_field(\'?zerovm_status     \', \'required\')\nretrieve_mnfst_field(\'?etag              \', \'required\')\nretrieve_mnfst_field(\'?#retcode          \', \'optional\')\nretrieve_mnfst_field(\'?#status           \', \'optional\')    \nretrieve_mnfst_field(\'?#content-type     \', \'optional\')\n# todo handle meta-tags\n# retrieve_mnfst_field(\'?#x-data-attr      \', \'optional\')\nimport cPickle\ninf = file(mnfst.input, \'r\')\nouf = file(mnfst.output, \'w\')\nid = cPickle.load(inf)\nod = cPickle.dumps(eval(file(mnfst.nexe, \'r\').read()))\nouf.write(od)\nfrom hashlib import md5\netag = md5()\netag.update(od)\netag = etag.hexdigest()\ninf.close()\nouf.close()\noufm = file(mnfst.output_mnfst,\'w\')\noufm.write(  \n        \'zerovm_retcode     =%s\\n\'\n        \'zerovm_status      =%s\\n\'\n        \'etag               =%s\\n\'\n        \'#retcode           =%s\\n\'\n        \'#status            =%s\\n\'\n        \'#content-type      =%s\\n\'\n        \'#x-data-attr       =Message:Hi\\n\'\n        \'#x-data-attr       =Base:still 10\\n\'\n        \'#x-data-attr       =AnotherMessage:blahblahblah\\n\'\n         % (retcode, status, etag, retcode, status, \'text\x2Fplain\'))\noufm.close()\nsys.exit(retcode)')

    def zerovm_request(self):
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'Content-Type': 'application/octet-stream',
                     'x-zerovm-execute': '1.0'})
        return req

    def test_QUERY_sort(self):
        self.setup_zerovm_query()
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript
        resp = req.get_response(self.app)

        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, self._sortednumbers)
        self.assertEquals(resp.content_length, len(self._sortednumbers))
        self.assertEquals(resp.content_type, 'text/plain')
        self.assertEquals(resp.headers['content-length'],
            str(len(self._sortednumbers)))
        self.assertEquals(resp.headers['X-Object-Meta-Message'], 'Hi')
        self.assertEquals(resp.headers['X-Object-Meta-Base'], 'still 10')
        self.assertEquals(resp.headers['X-Object-Meta-AnotherMessage'],
            'blahblahblah')
        self.assertEquals(resp.headers['content-type'], 'text/plain')
        self.assertEquals(resp.headers['etag'], self._sortednumbers_etag)
        self.assertEquals(resp.headers['x-query-nexe-retcode'], 0)
        self.assertEquals(resp.headers['x-query-nexe-status'], 'ok.')
        timestamp = normalize_timestamp(time())
        self.assertEquals(math.floor(float(resp.headers['X-Timestamp'])),
            math.floor(float(timestamp)))

    def test_QUERY_object_not_exists(self):
        # check if querying non existent object
        req = self.zerovm_request()
        req.body = ('SCRIPT')
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 404)

    def test_QUERY_invalid_path(self):
        # check if querying account or container fails also
        req = Request.blank('/sda1/p/a',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'x-zerovm-execute': '1.0'})
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 400)
        req = Request.blank('/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'x-zerovm-execute': '1.0'})
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 400)

    def test_QUERY_max_upload_time(self):
        class SlowBody():
            def __init__(self, _nexescript):
                self._nexescript = _nexescript
                self.sent = 0

            def read(self, size=-1):
                if self.sent < len(self._nexescript):
                    sleep(0.01)
                    self.sent += 1
                    return self._nexescript[self.sent - 1]
                return ''

        req = self.setup_zerovm_query()
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST',
                     'wsgi.input': SlowBody(self._nexescript)},
            headers={'Content-Type': 'application/octet-stream',
                     'x-zerovm-execute': '1.0',
                     'Content-Length': len(self._nexescript)})
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)
        orig_max_upload_time = self.app.app
        self.obj_controller.max_upload_time = 0.1
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST',
                     'wsgi.input': SlowBody(self._nexescript)},
            headers={'Content-Type': 'application/octet-stream',
                     'x-zerovm-execute': '1.0',
                     'Content-Length': len(self._nexescript)})
        resp = req.get_response(self.app)
        self.obj_controller.max_upload_time = orig_max_upload_time
        self.assertEquals(resp.status_int, 408)

    def test_QUERY_no_content_type(self):
        req = self.zerovm_request()
        del req.headers['Content-Type']
        req.body = ('SCRIPT')
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 400)
        self.assert_('No content type' in resp.body)

    def test_QUERY_invalid_content_type(self):
        req = self.zerovm_request()
        req.headers['Content-Type'] = 'application/blah-blah-blah'
        req.body = ('SCRIPT')
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 400)
        self.assert_('Invalid Content-Type' in resp.body)

    def test_QUERY_no_content_length(self):
        req = self.zerovm_request()
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 411)

    def test_QUERY_script_no_etag(self):
        self.setup_zerovm_query()
        req = self.zerovm_request()
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, self._sortednumbers)
        self.assertEquals(resp.headers['etag'], self._sortednumbers_etag)

    def test_QUERY_script_valid_etag(self):
        self.setup_zerovm_query()
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, self._sortednumbers)
        self.assertEquals(resp.headers['etag'], self._sortednumbers_etag)

    def test_QUERY_script_invalid_etag(self):
        self.setup_zerovm_query()
        etag = md5()
        etag.update('blah-blah-blah')
        etag = etag.hexdigest()
        req = self.zerovm_request()
        req.headers['etag'] = etag
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 422)

    def test_QUERY_short_body(self):
        class ShortBody():
            def __init__(self):
                self.sent = False

            def read(self, size=-1):
                if not self.sent:
                    self.sent = True
                    return '   '
                return ''

        self.setup_zerovm_query()
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST', 'wsgi.input': ShortBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'x-zerovm-execute': '1.0',
                     'Content-Length': '4',
                     'Content-Type': 'application/octet-stream'})
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 499)

    def test_QUERY_long_body(self):
        class LongBody():
            def __init__(self):
                self.sent = False

            def read(self, size=-1):
                if not self.sent:
                    self.sent = True
                    return '   '
                return ''

        self.setup_zerovm_query()
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST', 'wsgi.input': LongBody()},
            headers={'X-Timestamp': normalize_timestamp(time()),
                     'x-zerovm-execute': '1.0',
                     'Content-Length': '2',
                     'Content-Type': 'application/octet-stream'})
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 499)

    def test_QUERY_zerovm_term_timeouts(self):
        self.setup_zerovm_query()
        req = self.zerovm_request()
        req.body = 'sleep(10)'
        orig_timeout = None
        raised = False
        # call QUERY method
        try:
            orig_timeout = None if not \
            hasattr(self.app, 'zerovm_timeout') else \
            self.app.zerovm_timeout
            self.app.zerovm_timeout = 1
            self.app.zerovm_query(req)
        except Exception, e:
            self.assertEquals(str(e)[:16], 'ERROR OBJ.QUERY ')
            raised = True
        finally:
            self.app.zerovm_timeout = orig_timeout
        self.assert_(raised, 'Exception not raised on timeout')

    def test_QUERY_zerovm_kill_timeouts(self):
        self.setup_zerovm_query('import signal, time\nsignal.signal(signal.SIGTERM, signal.SIG_IGN )\ntime.sleep(10)')
        req = self.zerovm_request()
        req.body = 'bla bla bla'
        orig_timeout = None
        raised = False
        # call QUERY method
        try:
            orig_timeout = None if not\
            hasattr(self.app, 'zerovm_timeout') else\
            self.app.zerovm_timeout
            self.app.zerovm_timeout = 1
            orig_kill_timeout = None if not\
            hasattr(self.app, 'zerovm_kill_timeout') else\
            self.app.zerovm_kill_timeout
            self.app.zerovm_kill_timeout = 1
            self.app.zerovm_query(req)
        except Exception, e:
            self.assertEquals(str(e)[:16], 'ERROR OBJ.QUERY ')
            raised = True
        finally:
            self.app.zerovm_timeout = orig_timeout
            self.app.zerovm_kill_timeout = orig_kill_timeout
        self.assert_(raised, 'Exception not raised on timeout')

    def test_QUERY_simulteneous_running_zerovm_limits(self):
        from copy import copy

        self.setup_zerovm_query()
        slownexe = 'sleep(.1)'
        maxreq = 10 # must be divisible by 5
        r = range(0, maxreq)
        req = copy(r)
        orig_timeout = None
        orig_zerovm_maxqueue = None
        orig_zerovm_maxpool = None
        resp = None
        try:
            orig_zerovm_maxqueue = None if not\
            hasattr(self.app, 'zerovm_maxqueue') else\
            self.app.zerovm_maxqueue
            orig_zerovm_maxpool = None if not\
            hasattr(self.app, 'zerovm_maxpool') else\
            self.app.zerovm_maxpool
            orig_timeout = None if not\
            hasattr(self.app, 'zerovm_timeout') else\
            self.app.zerovm_timeout
            self.app.zerovm_timeout = 5
            from eventlet import GreenPool

            pool = GreenPool()
            t = copy(r)

            def make_requests_storm(queue_factor, pool_factor):
                from webob.exc import HTTPOk
                from webob.exc import HTTPServiceUnavailable

                for i in r:
                    req[i] = self.zerovm_request()
                    req[i].body = slownexe
                self.app.zerovm_maxqueue =\
                    int(maxreq * queue_factor)
                self.app.zerovm_maxpool =\
                    int(maxreq * pool_factor)
                spil_over = self.app.zerovm_maxqueue\
                    + self.app.zerovm_maxpool
                for i in r:
                    t[i] = pool.spawn(self.app.zerovm_query, req[i])
                pool.waitall()
                resp = copy(r)
                for i in r[:spil_over]:
                    resp[i] = t[i].wait()
                    print 'expecting ok #%s: %s' % (i, resp[i])
                    self.assertEquals(resp[i].status_int, 200)
                for i in r[spil_over:]:
                    resp[i] = t[i].wait()
                    print 'expecting fail #%s: %s' % (i, resp[i])
                    self.assertTrue(isinstance(resp[i], HTTPServiceUnavailable))

            make_requests_storm(0.2, 0.4)
            make_requests_storm(0, 1)
            make_requests_storm(0.4, 0.6)
            make_requests_storm(0, 0.1)

        finally:
            self.app.zerovm_timeout = orig_timeout
            self.app.zerovm_maxqueue = orig_zerovm_maxqueue
            self.app.zerovm_maxpool = orig_zerovm_maxpool

    def test_QUERY_failing_zerovms(self):
        self.setup_zerovm_query()
        orig_params = self.app.zerovm_nexe_xparams
        self.app.zerovm_nexe_xparams = ['fail:', '10']
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript
        raised = False
        try:
            self.app.zerovm_query(req)
        except Exception, e:
            self.assertEquals(str(e),
                'ERROR OBJ.QUERY zerovm_retcode=10, zerovm_status=fail:')
            raised = True
        finally:
            self.app.zerovm_xparams = orig_params
        self.assert_(raised, "Exception not raised")

    def test_QUERY_zerovm_maxmnfstline(self):
        def get_zerovm_maxmnfstline():
            self.setup_zerovm_query()
            req = self.zerovm_request()
            req.body = self._nexescript
            self.app.zerovm_query(req)
            return getattr(self.app, 'zerovm_maxmnfstline')

        self.setup_zerovm_query()
        orig_line = get_zerovm_maxmnfstline()
        setattr(self.app, 'zerovm_maxmnfstline', 0)
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript

        raised = False
        try:
            self.app.zerovm_query(req)
        except Exception, e:
            self.assertEquals(str(e), 'Input manifest must be smaller than 0.')
            raised = True
        finally:
            setattr(self.app, 'zerovm_maxmnfstline', orig_line)
        self.assert_(raised, "Exception not raised")

    def test_QUERY_zerovm_maxmnfstlines(self):
        def get_zerovm_maxmnfstlines():
            self.setup_zerovm_query()
            req = self.zerovm_request()
            req.body = self._nexescript
            self.app.zerovm_query(req)
            return getattr(self.app, 'zerovm_maxmnfstlines')

        self.setup_zerovm_query()
        orig_lines = get_zerovm_maxmnfstlines()
        setattr(self.app, 'zerovm_maxmnfstlines', 0)
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript

        raised = False
        try:
            self.app.zerovm_query(req)
        except Exception, e:
            self.assertEquals(str(e), 'Input manifest must be smaller than 0.')
            raised = True
        finally:
            setattr(self.app, 'zerovm_maxmnfstlines', orig_lines)
        self.assert_(raised, "Exception not raised")

    def test_QUERY_malformed_mnfst(self):
        def test(err, zvm_mock):
            self.setup_zerovm_query(zvm_mock)
            req = self.zerovm_request()
            req.body = self._nexescript

            raised = False
            try:
                self.app.zerovm_query(req)
            except Exception, e:
                self.assertEquals(str(e), err)
                raised = True
            self.assert_(raised, "Exception not raised")

        #empty manifest
        test('omnfst: expecting zerovm_retcode      got , retcode=0, status=>normal completion:', 'import sys\nsys.exit(0)')

        #malformed first line
        test('omnfst: expecting = at zerovm_retcode      got   retcode=0, status=>normal completion:param status:ok.\n\nparam retcode:0\n\n0\n', 'import signal\nfrom eventlet import sleep\nfrom sys import argv, exit \nif len(argv) \x3C 2 or len(argv) \x3E 5:\n    raise Exception(\'Incorrect number of arguments\')\nif argv[1] != \'-Y2\':\n    raise Exception(\'Invalid first argument: %s\' % sys.argv[1])\nif argv[2][:2] != \'-M\':\n    raise Exception(\'Invalid second argument: %s\' % sys.argv[2])\nmanifest = argv[2][2:]\ninputmnfst = file(manifest, \'r\').readlines()\nclass Mnfst:\n    pass\nmnfst = Mnfst()\nindex = 0\nstatus = \'ok.\' if len(argv) \x3C 3 else argv[3]\nprint \'param status:\' + str(status) + \'\\n\'\nretcode = 0 if len(argv) \x3C 4 else argv[4]\nprint \'param retcode:\' + str(retcode) + \'\\n\'\ndef retrieve_mnfst_field(n, vv = None, isint=False, rgx=None):\n    global index                  \n    if len(inputmnfst) \x3C index:\n        raise Exception(\'missing  %s\' % n)\n    if not inputmnfst[index][0:19] == n:\n        raise Exception(\'expecting %s got %s\' \n                        % (n, inputmnfst[index][0:19]))       \n    if not inputmnfst[index][19] == \'=\':\n        raise Exception(\'missing \\\'=\\\' at %s got %s\' \n                        % (n, inputmnfst[index][19]))       \n    v = inputmnfst[index][20:-1]\n    if isint:\n        v = int(v)\n    if rgx:\n        import re\n        if not re.match(rgx,v):\n            raise Exception(\'mnfst: %s does not match %s\' % n, rgx)\n    if n[0] == \'?\':\n        n = n[1:]\n    if vv and vv != v:\n        raise Exception(\'for %s expected %s got %s\' % (n, vv, v))\n    index += 1\n    setattr(mnfst, n.strip(), v)\nretrieve_mnfst_field(\'version            \',\'11nov2011\')\nretrieve_mnfst_field(\'zerovm             \')\nretrieve_mnfst_field(\'nexe               \')\nretrieve_mnfst_field(\'maxnexe            \', 256*1048576, True)\nretrieve_mnfst_field(\'input              \')\nretrieve_mnfst_field(\'maxinput           \', 256*1048576, True)\nretrieve_mnfst_field(\'#etag              \')\nretrieve_mnfst_field(\'#content-type      \')\nretrieve_mnfst_field(\'#x-timestamp       \')\nretrieve_mnfst_field(\'input_mnfst        \')\nretrieve_mnfst_field(\'output             \')\nretrieve_mnfst_field(\'maxoutput          \',64*1048576, True)\nretrieve_mnfst_field(\'output_mnfst       \')\nretrieve_mnfst_field(\'maxmnfstline       \', 1024, True)\nretrieve_mnfst_field(\'maxmnfstlines      \', 128, True)\nretrieve_mnfst_field(\'timeout            \', isint=True)\nretrieve_mnfst_field(\'kill_timeout       \', isint=True)\nretrieve_mnfst_field(\'?zerovm_retcode    \', \'required\')\nretrieve_mnfst_field(\'?zerovm_status     \', \'required\')\nretrieve_mnfst_field(\'?etag              \', \'required\')\nretrieve_mnfst_field(\'?#retcode          \', \'optional\')\nretrieve_mnfst_field(\'?#status           \', \'optional\')    \nretrieve_mnfst_field(\'?#content-type     \', \'optional\')\n# todo handle meta-tags\n# retrieve_mnfst_field(\'?#x-data-attr      \', \'optional\')\nimport cPickle\ninf = file(mnfst.input, \'r\')\nouf = file(mnfst.output, \'w\')\nid = cPickle.load(inf)\nod = cPickle.dumps(eval(file(mnfst.nexe, \'r\').read()))\nouf.write(od)\nfrom hashlib import md5\netag = md5()\netag.update(od)\netag = etag.hexdigest()\ninf.close()\nouf.close()\noufm = file(mnfst.output_mnfst,\'w\')\noufm.write(  \n        \'zerovm_retcode                     =%s\\n\'\n        \'zerovm_status      =%s\\n\'\n        \'etag               =%s\\n\'\n        \'#retcode           =%s\\n\'\n        \'#status            =%s\\n\'\n        \'#content-type      =%s\\n\'\n        \'#x-data-attr       =Message:Hi\\n\'\n        \'#x-data-attr       =Base:still 10\\n\'\n        \'#x-data-attr       =AnotherMessage:blahblahblah\\n\'\n         % (retcode, status, etag, retcode, status, \'text\x2Fplain\'))\noufm.close()\nexit(retcode)\n')

        #invalid etag
        test('mnfst: etag                does not match ([a-fA-F\d]{32})', 'import signal\nfrom eventlet import sleep\nfrom sys import argv \nif len(argv) \x3C 2 or len(argv) \x3E 5:\n    raise Exception(\'Incorrect number of arguments\')\nif argv[1] != \'-Y2\':\n    raise Exception(\'Invalid first argument: %s\' % sys.argv[1])\nif argv[2][:2] != \'-M\':\n    raise Exception(\'Invalid second argument: %s\' % sys.argv[2])\nmanifest = argv[2][2:]\ninputmnfst = file(manifest, \'r\').readlines()\nclass Mnfst:\n    pass\nmnfst = Mnfst()\nindex = 0\nstatus = \'ok.\' if len(argv) \x3C 3 else argv[3]\nprint \'param status:\' + str(status) + \'\\n\'\nretcode = 0 if len(argv) \x3C 4 else argv[4]\nprint \'param retcode:\' + str(retcode) + \'\\n\'\ndef retrieve_mnfst_field(n, vv = None, isint=False, rgx=None):\n    global index                  \n    if len(inputmnfst) \x3C index:\n        raise Exception(\'missing  %s\' % n)\n    if not inputmnfst[index][0:19] == n:\n        raise Exception(\'expecting %s got %s\' \n                        % (n, inputmnfst[index][0:19]))       \n    if not inputmnfst[index][19] == \'=\':\n        raise Exception(\'missing \\\'=\\\' at %s got %s\' \n                        % (n, inputmnfst[index][19]))       \n    v = inputmnfst[index][20:-1]\n    if isint:\n        v = int(v)\n    if rgx:\n        import re\n        if not re.match(rgx,v):\n            raise Exception(\'mnfst: %s does not match %s\' % n, rgx)\n    if n[0] == \'?\':\n        n = n[1:]\n    if vv and vv != v:\n        raise Exception(\'for %s expected %s got %s\' % (n, vv, v))\n    index += 1\n    setattr(mnfst, n.strip(), v)\nretrieve_mnfst_field(\'version            \',\'11nov2011\')\nretrieve_mnfst_field(\'zerovm             \')\nretrieve_mnfst_field(\'nexe               \')\nretrieve_mnfst_field(\'maxnexe            \', 256*1048576, True)\nretrieve_mnfst_field(\'input              \')\nretrieve_mnfst_field(\'maxinput           \', 256*1048576, True)\nretrieve_mnfst_field(\'#etag              \')\nretrieve_mnfst_field(\'#content-type      \')\nretrieve_mnfst_field(\'#x-timestamp       \')\nretrieve_mnfst_field(\'input_mnfst        \')\nretrieve_mnfst_field(\'output             \')\nretrieve_mnfst_field(\'maxoutput          \',64*1048576, True)\nretrieve_mnfst_field(\'output_mnfst       \')\nretrieve_mnfst_field(\'maxmnfstline       \', 1024, True)\nretrieve_mnfst_field(\'maxmnfstlines      \', 128, True)\nretrieve_mnfst_field(\'timeout            \', isint=True)\nretrieve_mnfst_field(\'kill_timeout       \', isint=True)\nretrieve_mnfst_field(\'?zerovm_retcode    \', \'required\')\nretrieve_mnfst_field(\'?zerovm_status     \', \'required\')\nretrieve_mnfst_field(\'?etag              \', \'required\')\nretrieve_mnfst_field(\'?#retcode          \', \'optional\')\nretrieve_mnfst_field(\'?#status           \', \'optional\')    \nretrieve_mnfst_field(\'?#content-type     \', \'optional\')\n# todo handle meta-tags\n# retrieve_mnfst_field(\'?#x-data-attr      \', \'optional\')\nimport cPickle\ninf = file(mnfst.input, \'r\')\nouf = file(mnfst.output, \'w\')\nid = cPickle.load(inf)\nod = cPickle.dumps(eval(file(mnfst.nexe, \'r\').read()))\nouf.write(od)\nfrom hashlib import md5\netag = md5()\netag.update(od)\netag = etag.hexdigest()\ninf.close()\nouf.close()\noufm = file(mnfst.output_mnfst,\'w\')\noufm.write(  \n        \'zerovm_retcode     =%s\\n\'\n        \'zerovm_status      =%s\\n\'\n        \'etag               =%s\\n\'\n        \'#retcode           =%s\\n\'\n        \'#status            =%s\\n\'\n        \'#content-type      =%s\\n\'\n        \'#x-data-attr       =Message:Hi\\n\'\n        \'#x-data-attr       =Base:still 10\\n\'\n        \'#x-data-attr       =AnotherMessage:blahblahblah\\n\'\n         % (retcode, status, \"$$$\", retcode, status, \'text\x2Fplain\'))\noufm.close()\nsys.exit(retcode)\n')

        #non zero retcode
        test('ERROR OBJ.QUERY zerovm_retcode=99, zerovm_status=ok.', 'import signal\nfrom eventlet import sleep\nfrom sys import argv \nif len(argv) \x3C 2 or len(argv) \x3E 5:\n    raise Exception(\'Incorrect number of arguments\')\nif argv[1] != \'-Y2\':\n    raise Exception(\'Invalid first argument: %s\' % sys.argv[1])\nif argv[2][:2] != \'-M\':\n    raise Exception(\'Invalid second argument: %s\' % sys.argv[2])\nmanifest = argv[2][2:]\ninputmnfst = file(manifest, \'r\').readlines()\nclass Mnfst:\n    pass\nmnfst = Mnfst()\nindex = 0\nstatus = \'ok.\' if len(argv) \x3C 3 else argv[3]\nprint \'param status:\' + str(status) + \'\\n\'\nretcode = 0 if len(argv) \x3C 4 else argv[4]\nprint \'param retcode:\' + str(retcode) + \'\\n\'\ndef retrieve_mnfst_field(n, vv = None, isint=False, rgx=None):\n    global index                  \n    if len(inputmnfst) \x3C index:\n        raise Exception(\'missing  %s\' % n)\n    if not inputmnfst[index][0:19] == n:\n        raise Exception(\'expecting %s got %s\' \n                        % (n, inputmnfst[index][0:19]))       \n    if not inputmnfst[index][19] == \'=\':\n        raise Exception(\'missing \\\'=\\\' at %s got %s\' \n                        % (n, inputmnfst[index][19]))       \n    v = inputmnfst[index][20:-1]\n    if isint:\n        v = int(v)\n    if rgx:\n        import re\n        if not re.match(rgx,v):\n            raise Exception(\'mnfst: %s does not match %s\' % n, rgx)\n    if n[0] == \'?\':\n        n = n[1:]\n    if vv and vv != v:\n        raise Exception(\'for %s expected %s got %s\' % (n, vv, v))\n    index += 1\n    setattr(mnfst, n.strip(), v)\nretrieve_mnfst_field(\'version            \',\'11nov2011\')\nretrieve_mnfst_field(\'zerovm             \')\nretrieve_mnfst_field(\'nexe               \')\nretrieve_mnfst_field(\'maxnexe            \', 256*1048576, True)\nretrieve_mnfst_field(\'input              \')\nretrieve_mnfst_field(\'maxinput           \', 256*1048576, True)\nretrieve_mnfst_field(\'#etag              \')\nretrieve_mnfst_field(\'#content-type      \')\nretrieve_mnfst_field(\'#x-timestamp       \')\nretrieve_mnfst_field(\'input_mnfst        \')\nretrieve_mnfst_field(\'output             \')\nretrieve_mnfst_field(\'maxoutput          \',64*1048576, True)\nretrieve_mnfst_field(\'output_mnfst       \')\nretrieve_mnfst_field(\'maxmnfstline       \', 1024, True)\nretrieve_mnfst_field(\'maxmnfstlines      \', 128, True)\nretrieve_mnfst_field(\'timeout            \', isint=True)\nretrieve_mnfst_field(\'kill_timeout       \', isint=True)\nretrieve_mnfst_field(\'?zerovm_retcode    \', \'required\')\nretrieve_mnfst_field(\'?zerovm_status     \', \'required\')\nretrieve_mnfst_field(\'?etag              \', \'required\')\nretrieve_mnfst_field(\'?#retcode          \', \'optional\')\nretrieve_mnfst_field(\'?#status           \', \'optional\')    \nretrieve_mnfst_field(\'?#content-type     \', \'optional\')\n# todo handle meta-tags\n# retrieve_mnfst_field(\'?#x-data-attr      \', \'optional\')\nimport cPickle\ninf = file(mnfst.input, \'r\')\nouf = file(mnfst.output, \'w\')\nid = cPickle.load(inf)\nod = cPickle.dumps(eval(file(mnfst.nexe, \'r\').read()))\nouf.write(od)\nfrom hashlib import md5\netag = md5()\netag.update(od)\netag = etag.hexdigest()\ninf.close()\nouf.close()\noufm = file(mnfst.output_mnfst,\'w\')\noufm.write(  \n        \'zerovm_retcode     =%s\\n\'\n        \'zerovm_status      =%s\\n\'\n        \'etag               =%s\\n\'\n        \'#retcode           =%s\\n\'\n        \'#status            =%s\\n\'\n        \'#content-type      =%s\\n\'\n        \'#x-data-attr       =Message:Hi\\n\'\n        \'#x-data-attr       =Base:still 10\\n\'\n        \'#x-data-attr       =AnotherMessage:blahblahblah\\n\'\n         % (99, status, etag, retcode, status, \'text\x2Fplain\'))\noufm.close()\nsys.exit(retcode)\n')

    def test_QUERY_max_input_size(self):
        self.setup_zerovm_query()
        orig_maxinput = getattr(self.app, 'zerovm_maxinput')
        setattr(self.app, 'zerovm_maxinput', 0)
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 413)
        setattr(self.app, 'zerovm_maxinput', orig_maxinput)

    def test_QUERY_max_nexe_size(self):
        self.setup_zerovm_query()
        orig_maxnexe = getattr(self.app, 'zerovm_maxnexe')
        setattr(self.app, 'zerovm_maxnexe', 0)
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 413)
        setattr(self.app, 'zerovm_maxnexe', orig_maxnexe)

    def test_QUERY_mnfst(self):
        zerovm_mock = (
            '    \nfrom eventlet import sleep\nfrom sys import argv \nif len(argv) \x3C 2 or len(argv) \x3E 5:\n    raise Exception(\'Incorrect number of arguments\')\nif argv[1] != \'-Y2\':\n    raise Exception(\'Invalid first argument: %s\' % sys.argv[1])\nif argv[2][:2] != \'-M\':\n    raise Exception(\'Invalid second argument: %s\' % sys.argv[2])\nmanifest = argv[2][2:]\ninputmnfst = file(manifest, \'r\').readlines()\nclass Mnfst:\n    pass\nmnfst = Mnfst()\nindex = 0\nstatus = \'ok.\' if len(argv) \x3C 3 else argv[3]\nprint \'param status:\' + str(status) + \'\\n\'\nretcode = 0 if len(argv) \x3C 4 else argv[4]\nprint \'param retcode:\' + str(retcode) + \'\\n\'\ndef retrieve_mnfst_field(n, vv = None, isint=False, rgx=None):\n    global index                  \n    if len(inputmnfst) \x3C index:\n        raise Exception(\'missing  %s\' % n)\n    if not inputmnfst[index][0:19] == n:\n        raise Exception(\'expecting %s got %s\' \n                        % (n, inputmnfst[index][0:19]))       \n    if not inputmnfst[index][19] == \'=\':\n        raise Exception(\'missing \\\'=\\\' at %s got %s\' \n                        % (n, inputmnfst[index][19]))       \n    v = inputmnfst[index][20:-1]\n    if isint:\n        v = int(v)\n    if rgx:\n        import re\n        if not re.match(rgx,v):\n            raise Exception(\'mnfst: %s does not match %s\' % n, rgx)\n    if n[0] == \'?\':\n        n = n[1:]\n    if vv and vv != v:\n        raise Exception(\'for %s expected %s got %s\' % (n, vv, v))\n    index += 1\n    setattr(mnfst, n.strip(), v)\nretrieve_mnfst_field(\'version            \',\'11nov2011\')\nretrieve_mnfst_field(\'zerovm             \')\nretrieve_mnfst_field(\'nexe               \')\nretrieve_mnfst_field(\'maxnexe            \', 256*1048576, True)\nretrieve_mnfst_field(\'input              \')\nretrieve_mnfst_field(\'maxinput           \', 256*1048576, True)\nretrieve_mnfst_field(\'#etag              \')\nretrieve_mnfst_field(\'#content-type      \')\nretrieve_mnfst_field(\'#x-timestamp       \')\nretrieve_mnfst_field(\'#x-data-attr       \',\'Message:Hello\')\nretrieve_mnfst_field(\'#x-data-attr       \',\'Base:10\')\nretrieve_mnfst_field(\'#x-data-attr       \',\'Format:Pickle\')\nnexe_attr = (\"Format:pickle\", \"Name:sorter\", \"Arg1:pickle\", \"Type:python\")\nfor i in range(4):\n    if inputmnfst[index][20:-1] in nexe_attr:\n        retrieve_mnfst_field(\"#x-nexe-attr       \",inputmnfst[index][20:-1])\n    else:\n        raise Exception(\"%s no in %s\" % (inputmnfst[index][20:-1], nexe_attr))\nretrieve_mnfst_field(\'input_mnfst        \')\nretrieve_mnfst_field(\'output             \')\nretrieve_mnfst_field(\'maxoutput          \',64*1048576, True)\nretrieve_mnfst_field(\'output_mnfst       \')\nretrieve_mnfst_field(\'maxmnfstline       \', 1024, True)\nretrieve_mnfst_field(\'maxmnfstlines      \', 128, True)\nretrieve_mnfst_field(\'timeout            \', isint=True)\nretrieve_mnfst_field(\'kill_timeout       \', isint=True)\nretrieve_mnfst_field(\'?zerovm_retcode    \', \'required\')\nretrieve_mnfst_field(\'?zerovm_status     \', \'required\')\nretrieve_mnfst_field(\'?etag              \', \'required\')\nretrieve_mnfst_field(\'?#retcode          \', \'optional\')\nretrieve_mnfst_field(\'?#status           \', \'optional\')    \nretrieve_mnfst_field(\'?#content-type     \', \'optional\')\n# todo handle meta-tags\n# retrieve_mnfst_field(\'?#x-data-attr      \', \'optional\')\nimport cPickle\ninf = file(mnfst.input, \'r\')\nouf = file(mnfst.output, \'w\')\nid = cPickle.load(inf)\nod = cPickle.dumps(eval(file(mnfst.nexe, \'r\').read()))\nouf.write(od)\nfrom hashlib import md5\netag = md5()\netag.update(od)\netag = etag.hexdigest()\ninf.close()\nouf.close()\noufm = file(mnfst.output_mnfst,\'w\')\noufm.write(  \n        \'zerovm_retcode     =%s\\n\'\n        \'zerovm_status      =%s\\n\'\n        \'etag               =%s\\n\'\n        \'#retcode           =%s\\n\'\n        \'#status            =%s\\n\'\n        \'#content-type      =%s\\n\'\n        \'#x-data-attr       =Message:Hi\\n\'\n        \'#x-data-attr       =Base:still 10\\n\'\n        \'#x-data-attr       =AnotherMessage:blahblahblah\\n\'\n         % (retcode, status, etag, retcode, status, \'text\x2Fplain\'))\noufm.close()\nsys.exit(retcode)\n\n')
        self.setup_zerovm_query(zerovm_mock, True)
        req = Request.blank('/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'Content-Type': 'application/octet-stream',
                     'etag': self._nexescript_etag,
                     'x-zerovm-execute': '1.0',
                     'X-Object-Meta-Type': 'python',
                     'X-Object-Meta-Name': 'sorter',
                     'X-Object-Meta-arg1': 'pickle',
                     'X-Object-Meta-format': 'pickle'})
        req.body = self._nexescript
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_QUERY_max_chunk_size(self):
        self.setup_zerovm_query()
        self.app.zerovm_maxchunksize = 10
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript
        resp = req.get_response(self.app)

        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, self._sortednumbers)

    def test_QUERY_mount_check(self):
        self.setup_zerovm_query()
        orig_mountcheck = getattr(self.obj_controller, 'mount_check')
        self.obj_controller.mount_check = True
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript
        resp = req.get_response(self.app)

        self.assertEquals(resp.status_int, 507)
        setattr(self.obj_controller, 'mount_check', orig_mountcheck)

    def test_QUERY_file_iter_OSError(self):
        class OsMock():
            def close(self, fd):
                self.closed = True
                raise OSError

            def unlink(self, fd):
                self.unlinked = True
                raise OSError

        # test scenario where file closing fails after data had been downloaded
        self.setup_zerovm_query()
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript
        resp = self.app.zerovm_query(req)
        orig_os = self.app.os_interface
        self.app.os_interface = OsMock()
        respbody = resp.body
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, self._sortednumbers)
        #test wheither files where attempted to be closed and deleted
        self.assertTrue(self.app.os_interface.closed)
        self.assertTrue(self.app.os_interface.unlinked)

        # test scenario where file allocation fails
        self.app.os_interface = OsMock()
        orig_zerovm_maxoutput = self.app.zerovm_maxoutput \
            if hasattr(self.app, 'zerovm_maxoutput') else None
        self.app.zerovm_maxoutput = 1024 * 1024 * 1024 * 1024 * 1024
        self.setup_zerovm_query()
        req = self.zerovm_request()
        req.headers['etag'] = self._nexescript_etag
        req.body = self._nexescript

        self.assertRaises(OSError, self.app.zerovm_query, req)
        #test wheither files where attempted to be closed and deleted
        self.assertTrue(self.app.os_interface.closed)
        self.assertTrue(self.app.os_interface.unlinked)

        self.app.os_interface = orig_os
        self.app.zerovm_maxoutput = orig_zerovm_maxoutput

    '''
    # this should be the last test
    def test_QUERY_real_zerovm(self):

        raise SkipTest
        #This test will work only if ZeroVM is properly installed on the system
        #at /home/dazo/ZeroVM

        timestamp = normalize_timestamp(time())
        req = Request.blank('/sda1/p/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                        headers={'X-Timestamp': timestamp,
                                 'Content-Type': 'application/octet-stream'})

        file = open('/home/dazo/ZeroVM/samples/sort_mapping/input.data', 'rb')
        data = file.read()
        req.body = data
        resp = self.object_controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        req = Request.blank('/sda1/p/a/c/o',
                        headers={'Content-Type': 'application/octet-stream'})
        file = open('/home/dazo/ZeroVM/samples/sort_mapping/sort_uint_proper_'
                    'with_args.c_x86_64.nexe', 'rb')
        code = file.read()
        req.body = code

        # call QUERY method
        resp = req.get_response(self.app)

        #check results
        self.assertEquals(resp.status_int, 200)

        file = open('/home/dazo/ZeroVM/samples/sort_mapping/output.data', 'rb')
        res = file.read()

        self.assertEquals(resp.body, res)
        self.assertEquals(resp.content_length, len(res))
        self.assertEquals(resp.content_type, 'application/octet-stream')
        self.assertEquals(resp.headers['content-length'], str(len(res)))
        self.assertEquals(resp.headers['content-type'],
                          'application/octet-stream')
        #TODOLE add etag support to this test
        #self.assertEquals(resp.headers['etag'], self._sortednumbers_etag)
        # --skipped, Leon.
    '''

if __name__ == '__main__':
    unittest.main()