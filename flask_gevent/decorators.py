from functools import wraps

from flask import current_app
from werkzeug.local import LocalProxy
from gevent._util import _NONE


def app_context(app):
    if isinstance(app, LocalProxy):
        app = app._get_current_object()

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with app.app_context():
                return func(*args, **kwargs)
        return wrapper
    return decorator


def with_timeout(seconds, warning=True, timeout_value=_NONE):
    # Actually same implementation as in gevent, but using decorator and with warning
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            timeout = Timeout.start_new(seconds, _one_shot=True)
            try:
                return func(*args, **kwargs)
            except gevent.Timeout  as t:
                if t is not timeout:
                    raise
                if warning:
                    current_app.logger.warning(
                        'Timeout %s exceeded on %r args=%s kwargs=%s',
                         seconds, func, args, kwargs)
                if timeout_value is not _NONE:
                    return timeout_value
                raise
            finally:
                timeout.cancel()
        return wrapper
    return decorator


def run_forever(self, sleep=0, exit_on_error=True):
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
