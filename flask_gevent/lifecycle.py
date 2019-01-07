import atexit

import gevent
import gevent.pool

from .decorators import app_context


def atexit_register(func):
    # https://github.com/opbeat/opbeat_python/blob/4bdfe4/opbeat/utils/compat.py#L23
    """
    Uses either uwsgi's atexit mechanism, or atexit from the stdlib.
    When running under uwsgi, using their atexit handler is more reliable,
    especially when using gevent
    :param func: the function to call at exit
    """
    try:
        import uwsgi
        orig = getattr(uwsgi, 'atexit', None)

        def uwsgi_atexit():
            if callable(orig):
                orig()
            func()

        uwsgi.atexit = uwsgi_atexit
    except ImportError:
        atexit.register(func)


class GeventLifecycle:
    app = None

    def __init__(self, app=None):
        self._signal, self._exit, self._run = [], [], []
        if app:
            self.init_app(app)

    def init_app(self, app):
        if self.app:
            raise RuntimeError('GeventLifecycle instance can be registered only once per app')
        self.app = app
        self.pool = gevent.pool.Pool()
        self._bind()
        # app.extensions['gevent_lifecycle'] = self

    def _bind(self):
        # This may run more than once because of registering
        # handlers after app initialization.
        for func, signalnums in self._signal:
            for signalnum in signalnums:
                gevent.signal_handler(app_context(self.app)(func))
        for func in self._run:
            self.pool.spawn(app_context(self.app)(func))
        for func in self._exit:
            atexit_register(app_context(self.app))
        self._signal.clear()
        self._run.clear()
        self._exit.clear()

    def _maybe_bind(self):
        if self.app:
            self._bind()

    def signal(self, signalnum, *signalnums):
        def decorator(func):
            self._signal.append(func, frozenset((signalnum,) + signalnums))
            self._maybe_bind()
            return func
        return decorator

    def exit(self, func):
        self._exit.append(func)
        self._maybe_bind()
        return func

    def run(self, func):
        self._run.append(func)
        self._maybe_bind()
        return func

    def run_forever(self, sleep=0):
        def decorator(func):
            self.run(run_forever(sleep)(func))
            return func
        return decorator
