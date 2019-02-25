import atexit
import sys

from flask import current_app
from werkzeug.utils import import_string
import gevent
import gevent.pool

from .utils import app_context


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


def _maybe_import(value):
    return import_string(value) if isinstance(value, str) else value


def _run_forever(self, sleep=0, exit_on_error=True):
    def decorator(func):
        def wrapper():
            while True:
                try:
                    func()
                except Exception as exc:
                    current_app.logger.exception('%r failed: %r', func, exc)
                    if exit_on_error:
                        print('%r failed: %r' % (func, exc), file=sys.stderr)
                        return sys.exit(1)
                if sleep:
                    current_app.logger.debug('%s sleeping for %s seconds', func, sleep)
                    gevent.sleep(sleep)
        return func
    return decorator


class GeventLifecycle:
    def __init__(self, app, signal=[], exit=[], run=[], run_forever=[]):
        self.app = app
        self.pool = gevent.pool.Pool()

        self._signal = [(_maybe_import(func), *args) for func, *args in signal]
        self._exit = [_maybe_import(func) for func in exit]
        self._run = (
            [_maybe_import(func) for func in run]
            + [_run_forever(*args)(_maybe_import(func))
               for func, *args in run_forever]
        )
        self._bind()

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

    def signal(self, signalnums):
        def decorator(func):
            self._signal.append(func, frozenset(signalnums))
            self._bind()
            return func
        return decorator

    def exit(self, func):
        self._exit.append(func)
        self._bind()
        return func

    def run(self, func):
        self._run.append(func)
        self._bind()
        return func

    def run_forever(self, sleep=0, exit_on_error=True):
        def decorator(func):
            self.run(_run_forever(sleep, exit_on_error)(func))
            return func
        return decorator
