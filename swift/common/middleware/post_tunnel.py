from swift.common.swob import Request

class HttpMethodOverrideMiddleware(object):

    def __init__(self, application,
                 header_name='X-HTTP-Method-Override',
                 authorized_methods=('PUT', 'DELETE', 'OPTIONS', 'PATCH')):

        if not callable(application):
            raise TypeError('Application must be callable, but {0!r} is not '
                            'callable'.format(application))
        self.application = application
        self.header_name = header_name
        self.authorized_methods = authorized_methods

    def __call__(self, environ, start_response):
        if environ['REQUEST_METHOD'] == 'POST':
            request = Request(environ)
            if self.header_name in request.headers:
                environ['REQUEST_METHOD'] = request.headers.get(self.header_name).upper()
        return self.application(environ, start_response)


def filter_factory(global_conf, **local_conf):
    """
    paste.deploy app factory for creating WSGI proxy apps.
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def method_override_filter(app):
        return HttpMethodOverrideMiddleware(app)
    return method_override_filter