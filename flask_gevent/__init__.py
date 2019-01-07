from copy import copy

import gevent
import gevent.greenlet
import gevent.pool
from flask import current_app

from .decorators import app_context
from .lifecycle import GeventLifecycle


class Greenlet(gevent.greenlet.Greenlet):
    _app = current_app

    def __init__(self, run=None, *args, **kwargs):
        super().__init__(run, *args, **kwargs)
        self._run = app_context(self._app)(self._run)


class Group(gevent.pool.Group):
    greenlet_class = Greenlet


class Pool(gevent.pool.Pool):
    greenlet_class = Greenlet


class Gevent(object):
    """
    A Flask extension to ease work with gevent (mainly with app_context).

    :param app: a :class:`~flask.Flask` instance. Defaults to `None`. If no
        application is provided on creation, then it can be provided later on
        via :meth:`init_app`.
    """
    app = None
    pools = None
    pool_class = Pool
    greenlet_class = Greenlet

    def __init__(self, app=None, **options):
        self.lifecycle = GeventLifecycle()
        if app is not None:
            self.init_app(app, **options)
        elif options:
            raise TypeError('You can pass **options only on app initialization')

    def init_app(self, app, pools={}, lifecycle=True):
        if self.app is not None:
            raise RuntimeError('Gevent instance can be registered only once per app')
        self.app = app

        # em... do we really need this bind?
        self.greenlet_class = copy(self.greenlet_class)
        self.greenlet_class._app = app
        self.pool_class.greenlet_class = self.greenlet_class

        self.pools = {
            name: (
                pool if isinstance(pool, gevent.pool.Group)
                else self.pool_class(**{'greenlet_class': self.greenlet_class, **pool})
            )
            for name, pool in pools.items()
        }
        app.extensions['gevent'] = self
        if lifecycle:
            self.lifecycle.init_app(app)
        return self

    def _get_app(self):
        if current_app:
            return current_app._get_current_object()
        elif self.app is not None:
            return self.app
        raise RuntimeError('No application found')

    def app_context(self):
        return app_context(self._get_app())

    def spawn(self, func, *args, **kwargs):
        return self.greenlet_class.spawn(self.app_context()(func), *args, **kwargs)

    def spawn_later(self, seconds, func, *args, **kwargs):
        return self.greenlet_class.spawn_later(seconds, self.app_context()(func), *args, **kwargs)

    def spawn_raw(self, func, *args, **kwargs):
        return gevent.spawn_raw(self.app_context()(func), *args, **kwargs)

    def signal_handler(self, signalnum, handler, *args, **kwargs):
        return gevent.signal_handler(signalnum, self.app_context()(handler),
                                     *args, **kwargs)

    # Lifecycle decorators
    signal = property(lambda self: self.lifecycle.signal)
    exit = property(lambda self: self.lifecycle.exit)
    run = property(lambda self: self.lifecycle.run)
    run_forever = property(lambda self: self.lifecycle.run_forever)
