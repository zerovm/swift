from Cookie import SimpleCookie
from urllib import quote, unquote
from time import gmtime, strftime, time
from urlparse import parse_qs
import datetime
from swift.common.internal_client import InternalClient

from swift.common.http import HTTP_CLIENT_CLOSED_REQUEST
from swift.common.oauth import Client
from swift.common.swob import HTTPFound, Response, Request, HTTPUnauthorized, HTTPForbidden, HTTPNotFound
from swift.common.utils import cache_from_env, get_logger, TRUE_VALUES, split_path
from swift.common.middleware.acl import clean_acl
from swift.common.bufferedhttp import http_connect

class LiteAuth(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.version = 'v1'
        self.google_client_id = conf.get('google_client_id')
        self.google_client_secret = conf.get('google_client_secret')
        self.service_domain = conf.get('service_domain')
        self.service_endpoint = conf.get('service_endpoint', 'https://' + self.service_domain)
        self.google_scope = conf.get('google_scope')
        #conf_path = conf.get('__file__')
        #self.swift = InternalClient(conf_path, 'LiteAuth', 3)
        self.google_auth = '/login/google/'
        self.google_prefix = 'g_'
        self.logger = get_logger(conf, log_route='lite-auth')
        self.log_headers = conf.get('log_headers', 'f').lower() in TRUE_VALUES

    def __call__(self, env, start_response):
        if env.get('PATH_INFO', '').startswith(self.google_auth):
            qs = env.get('QUERY_STRING')
            if qs:
                code = parse_qs(qs).get('code', None)
                if code:
                    if not 'eventlet.posthooks' in env:
                        req = Request(env)
                        req.bytes_transferred = '-'
                        req.client_disconnect = False
                        req.start_time = time()
                        response = self.do_google_login(env, code[0])(env, start_response)
                        self.posthooklogger(env, req)
                        return response
                    else:
                        return self.do_google_login(env, code[0])(env, start_response)
            return self.do_google_oauth(env, start_response)
        auth_token = None
        try:
            auth_token = SimpleCookie(env.get('HTTP_COOKIE',''))['session'].value
        except KeyError:
            pass
        if auth_token:
            env['HTTP_X_AUTH_TOKEN'] = auth_token
            env['HTTP_X_STORAGE_TOKEN'] = auth_token
        token = env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN', None))
        if token:
            user_data = self.get_cached_user_data(env, token)
            if user_data:
                env['REMOTE_USER'] = user_data
                env['HTTP_X_AUTH_TOKEN'] = '%s,%s' % (user_data, token)
                env['swift.authorize'] = self.authorize
                env['swift.clean_acl'] = clean_acl
            else:
                return HTTPUnauthorized()(env, start_response)
        else:
            env['swift.authorize'] = self.authorize
            env['swift.clean_acl'] = clean_acl
        return self.app(env, start_response)

    def do_google_oauth(self, env, start_response):
        c = Client(auth_endpoint='https://accounts.google.com/o/oauth2/auth',
            client_id=self.google_client_id,
            redirect_uri='%s%s' % (self.service_endpoint, self.google_auth))
        loc = c.auth_uri(scope=self.google_scope.split(','), access_type='offline')
        return HTTPFound(location=loc)(env, start_response)

    def do_google_login(self, env, code):
        if 'eventlet.posthooks' in env:
            req = Request(env)
            req.bytes_transferred = '-'
            req.client_disconnect = False
            req.start_time = time()
            env['eventlet.posthooks'].append(
                (self.posthooklogger, (req,), {}))
        c = Client(token_endpoint='https://accounts.google.com/o/oauth2/token',
            resource_endpoint='https://www.googleapis.com/oauth2/v1',
            redirect_uri='%s%s' % (self.service_endpoint, self.google_auth),
            client_id=self.google_client_id,
            client_secret=self.google_client_secret)
        c.request_token(code=code)
        token = c.access_token
        if hasattr(c, 'refresh_token'):
            rc = Client(token_endpoint=c.token_endpoint,
                client_id=c.client_id,
                client_secret=c.client_secret,
                resource_endpoint=c.resource_endpoint)

            rc.request_token(grant_type='refresh_token',
                refresh_token=c.refresh_token)
            token = rc.access_token
        if not token:
            return HTTPUnauthorized()
        user_data = self.get_new_user_data(env, c)
        if not user_data:
            return HTTPForbidden()
        cookie = SimpleCookie()
        cookie['session'] = token
        cookie['session']['path'] = '/'
        if not self.service_domain.startswith('localhost'):
            cookie['session']['domain'] = self.service_domain
        expiration = datetime.datetime.utcnow() + datetime.timedelta(seconds=c.expires_in)
        cookie['session']['expires'] = expiration.strftime('%a, %d %b %Y %H:%M:%S GMT')
        resp = Response(request=req, status=302,
            headers={
                'x-auth-token': token,
                'x-storage-token': token,
                'x-storage-url': '%s/%s/%s' % (self.service_endpoint, self.version, user_data),
                'set-cookie': cookie['session'].output(header='').strip(),
                'location': '%s/%s/%s' % (self.service_endpoint, self.version, user_data)})
        print resp.headers
        return resp

    def get_cached_user_data(self, env, token):
        user_data = None
        memcache_client = cache_from_env(env)
        if not memcache_client:
            raise Exception('Memcache required')
        memcache_token_key = '%s/token/%s' % (self.google_prefix, token)
        cached_auth_data = memcache_client.get(memcache_token_key)
        if cached_auth_data:
            expires, user_data = cached_auth_data
            if expires < time():
                user_data = None
        return user_data

    def get_new_user_data(self, env, client):
        user_data = client.request('/userinfo')
        if user_data:
            user_data = self.google_prefix + user_data.get('id')
            expires = time() + client.expires_in
            memcache_client = cache_from_env(env)
            memcache_token_key = '%s/token/%s' % (self.google_prefix, client.access_token)
            memcache_client.set(memcache_token_key, (expires, user_data),
                timeout=float(expires - time()))
        return user_data

    def authorize(self, req):
        try:
            version, account, container, obj = split_path(req.path, 1, 4, True)
        except ValueError:
            self.logger.increment('errors')
            return HTTPNotFound(request=req)
        if not account or not account.startswith(self.google_prefix):
            return self.denied_response(req)
        user_data = (req.remote_user or '')
        if account in user_data and\
           (req.method not in ('DELETE', 'PUT') or container):
            # If the user is admin for the account and is not trying to do an
            # account DELETE or PUT...
            req.environ['swift_owner'] = True
            return None
        return self.denied_response(req)

    def denied_response(self, req):
        if req.remote_user:
            self.logger.increment('forbidden')
            return HTTPForbidden(request=req)
        else:
            self.logger.increment('unauthorized')
            return HTTPUnauthorized(request=req)


    def posthooklogger(self, env, req):
        if not req.path.startswith(self.google_auth):
            return
        response = getattr(req, 'response', None)
        if not response:
            return
        trans_time = '%.4f' % (time() - req.start_time)
        the_request = quote(unquote(req.path))
        if req.query_string:
            the_request = the_request + '?' + req.query_string
            # remote user for zeus
        client = req.headers.get('x-cluster-client-ip')
        if not client and 'x-forwarded-for' in req.headers:
            # remote user for other lbs
            client = req.headers['x-forwarded-for'].split(',')[0].strip()
        logged_headers = None
        if self.log_headers:
            logged_headers = '\n'.join('%s: %s' % (k, v)
                for k, v in req.headers.items())
        status_int = response.status_int
        if getattr(req, 'client_disconnect', False) or\
           getattr(response, 'client_disconnect', False):
            status_int = HTTP_CLIENT_CLOSED_REQUEST
        self.logger.info(
            ' '.join(quote(str(x)) for x in (client or '-',
                     req.remote_addr or '-', strftime('%d/%b/%Y/%H/%M/%S', gmtime()),
                     req.method, the_request, req.environ['SERVER_PROTOCOL'],
                     status_int, req.referer or '-', req.user_agent or '-',
                     req.headers.get('x-auth-token',
                         req.headers.get('x-auth-admin-user', '-')),
                     getattr(req, 'bytes_transferred', 0) or '-',
                     getattr(response, 'bytes_transferred', 0) or '-',
                     req.headers.get('etag', '-'),
                     req.environ.get('swift.trans_id', '-'), logged_headers or '-',
                     trans_time)))


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def auth_filter(app):
        return LiteAuth(app, conf)
    return auth_filter