import gevent
import gevent.greenlet
import gevent.pool
from flask import current_app

from .utils import app_context, repr_pool_status
from .lifecycle import GeventLifecycle


class Greenlet(gevent.greenlet.Greenlet):
    _app = current_app

    def __init__(self, run=None, *args, **kwargs):
        super().__init__(run, *args, **kwargs)
        self._run = app_context(self._app)(self._run)


class Group(gevent.pool.Group):
    greenlet_class = Greenlet
    __str__ = repr_pool_status


class Pool(gevent.pool.Pool):
    greenlet_class = Greenlet
    __str__ = repr_pool_status


class _GeventState(object):
    def __init__(self, gevent, pools, lifecycle):
        self.gevent = gevent
        self.pools = pools
        self.lifecycle = lifecycle

    def __getattr__(self, name):
        return getattr(self.gevent, name)


class Gevent(object):
    """
    A Flask extension to ease work with gevent (mainly with app_context).

    :param app: a :class:`~flask.Flask` instance. Defaults to `None`. If no
        application is provided on creation, then it can be provided later on
        via :meth:`init_app`.
    """
    app = None
    pool_class = Pool
    greenlet_class = Greenlet

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app, pools={}, **lifecycle):
        pools = {
            name: (
                pool if isinstance(pool, gevent.pool.Group)
                else self.pool_class(**{'greenlet_class': self.greenlet_class, **pool})
            )
            for name, pool in {**app.conf.get('GEVENT_POOLS', {}), **pools}.items()
        }
        lifecycle = {**app.conf.get('GEVENT_LIFECYCLE', {}), **lifecycle}
        app.extensions['gevent'] = _GeventState(self, pools,
                                                GeventLifecycle(app, **lifecycle))

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
        return self.greenlet_class.spawn_later(seconds, self.app_context()(func),
                                               *args, **kwargs)

    def spawn_raw(self, func, *args, **kwargs):
        return gevent.spawn_raw(self.app_context()(func), *args, **kwargs)

    def signal_handler(self, signalnum, handler, *args, **kwargs):
        return gevent.signal_handler(signalnum, self.app_context()(handler),
                                     *args, **kwargs)

    @property
    def pools(self):
        self._get_app().extensions['gevent'].pools

    @property
    def lifecycle(self):
        self._get_app().extensions['gevent'].lifecycle
