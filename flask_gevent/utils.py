from flask import current_app

from .decorators import app_context


class Greenlet(gevent.greenlet.Greenlet):
    def __init__(self, run=None, *args, **kwargs):
        super().__init__(run, *args, **kwargs)
        self._run = app_context(current_app)(self._run)


class Group(gevent.pool.Group):
    greenlet_class = Greenlet


class Pool(gevent.pool.Pool):
    greenlet_class = Greenlet
