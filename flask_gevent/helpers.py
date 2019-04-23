from collections import UserDict

from flask import current_app
from werkzeug.utils import cached_property
from gevent import Timeout, joinall
from gevent.lock import Semaphore, BoundedSemaphore

from .utils import repr_pool_status


class EntityBulkProcessor:
    def __init__(self, pool, spawn_timeout=10, join_timeout=30,
                 worker=None, logger=None):
        self.pool = pool
        self.spawn_timeout = spawn_timeout
        self.join_timeout = join_timeout
        if worker:
            self._worker = worker
        self._logger = logger

        self.workers = {}

    @cached_property
    def logger(self):
        return self._logger or current_app.logger

    def __call__(self, *entity_ids,
                 spawn=True, spawn_timeout=None, spawn_raise=True,
                 join=False, join_timeout=None, join_raise=True,
                 as_dict=False):
        # Returns (data, errors), which are mappings by entity_id
        # spawn - starts worker for not found entity_id, skips otherwise
        # spawn_timeout - maximum wait to spawn, if False - waits forever
        # spawn_raise - raise Timeout on spawn_timeout
        # join - wait for workers to finish
        # join_timeout - maximum wait to finish, if False - waits forever
        # join_raise - raise RuntimeError on join_timeout
        # as_dict - return {data, errors} dict instead tuple

        # worker may:
        #   return value - on_value is triggered, data[entity_id] = value
        #   return None - on_value is not triggered, no entity_id in data or errors
        #   return Exception - on_exception is triggered, no traceback logged
        #   raise Exception - on_exception is triggered, traceback logged

        entity_ids = set(entity_ids)
        spawn_timeout = self.spawn_timeout if spawn_timeout is None else spawn_timeout
        join_timeout = self.join_timeout if join_timeout is None else join_timeout

        timeout = None
        if spawn and spawn_timeout is not False:
            timeout = Timeout.start_new(spawn_timeout)
        try:
            rv, workers = self._get_or_spawn(
                entity_ids, spawn, link=bool(join_timeout))
        except Timeout as exc:
            if exc is timeout:
                self.logger.warning('Update timeout: %s %s', exc, entity_ids)
                if spawn_raise:
                    raise
                rv, workers = self._get_or_spawn(entity_ids, False, False)
            else:
                raise
        finally:
            if timeout:
                timeout.cancel()

        self.logger.debug(
            'Processing: rv=%s spawned=%s pool=%s',
            set(rv.keys()) or '{}', [(w.args and w.args[0]) for w in workers],
            repr_pool_status(self.pool),
        )
        if workers and join:
            if join_timeout is not None and join_raise:
                timeout = Timeout.start_new(spawn_timeout)

            finished_workers = joinall(workers, timeout=join_timeout)
            if len(workers) != len(finished_workers):
                # w.args[0] is available only for not ready workers,
                # it's cleared otherwise
                args = [w.args for w in set(workers).difference(finished_workers)]
                self.logger.warning(
                    'Join timeout: %d, not finished workers: %s',
                    join_timeout, args)
                if join_raise:
                    raise RuntimeError('Join timeout exceeded',
                                       join_timeout, args)

        data, errors = {}, {}
        for entity_id, value in rv.items():
            if isinstance(value, Exception):
                errors[entity_id] = value
            else:
                data[entity_id] = value
        if as_dict:
            return {'data': data, 'errors': errors}
        return data, errors

    def _get_or_spawn(self, entity_ids, spawn, link=False):
        rv, workers = self.getter(entity_ids), []
        for entity_id in entity_ids.difference(rv.keys()):
            worker = self.workers.get(entity_id)
            if not worker and spawn:
                worker = self._spawn_worker(entity_id, (entity_id,))
            if worker:
                if link:
                    worker.link(self._create_link(rv, entity_id))
                workers.append(worker)
        return rv, workers

    def _create_link(self, rv, entity_id):
        def _link_greenlet(greenlet):
            if greenlet.successful():
                if greenlet.value is not None:
                    rv[entity_id] = greenlet.value
            else:
                rv[entity_id] = greenlet.exception
        return _link_greenlet

    def _spawn_worker(self, entity_id, args=(), kwargs={}):
        self.pool.wait_available()
        # For cases when several workers for same entity is waiting before spawn
        if entity_id not in self.workers:
            self.workers[entity_id] = self.pool.spawn(
                self.worker, entity_id, args, kwargs)
        return self.workers[entity_id]

    def worker(self, entity_id, args, kwargs):
        self.logger.debug('Starting worker: %s %s %s', entity_id, args, kwargs)
        try:
            rv = self._worker(*args, **kwargs)
        except Exception as exc:
            self.logger.exception('Worker failed: %s %s %s %r',
                                  entity_id, args, kwargs, exc)
            self.on_exception(entity_id, exc)
        except BaseException as exc:
            self.logger.debug('Worker failed: %s %s %s %r',
                              entity_id, args, kwargs, exc)
            raise
        else:
            if isinstance(rv, Exception):
                # Allowing returning error not to print traceback
                self.logger.warning('Worker failed: %s %s %s %r',
                                    entity_id, args, kwargs, rv)
                self.on_exception(entity_id, rv)
            elif rv is not None:
                self.on_value(entity_id, rv)
            return rv
        finally:
            del self.workers[entity_id]

    def getter(self, entity_ids):
        raise NotImplementedError()

    def on_value(self, entity_id, value):
        raise NotImplementedError()

    def on_exception(self, entity_id, exc):
        raise NotImplementedError()

    def _worker(self, entity_id):
        raise NotImplementedError()


class CacheEntityBulkProcessor(EntityBulkProcessor):
    def __init__(self, pool, cache, cache_timeout, cache_exception_timeout,
                 **kwargs):
        self.cache = cache
        self.cache_timeout = cache_timeout
        self.cache_exception_timeout = cache_exception_timeout
        super().__init__(pool, **kwargs)

    def getter(self, entity_ids):
        rv = {}
        for entity_id in entity_ids:
            data = self.cache.get(entity_id)
            if data is not None:
                rv[entity_id] = data
        return rv

    def on_value(self, entity_id, value):
        self.cache.set(entity_id, value, self.cache_timeout)

    def on_exception(self, entity_id, exc):
        if self.cache_exception_timeout:
            self.cache.set(entity_id, exc, self.cache_exception_timeout)

    def _worker(self, entity_id):
        raise NotImplementedError()


class SqlAlchemyEntityBulkProcessor(EntityBulkProcessor):
    def __init__(self, pool, model, id_field='id', commit=False, **kwargs):
        self.model = model
        self.id_field = id_field
        self.commit = commit
        super().__init__(pool, **kwargs)

    def getter(self, entity_ids):
        return {
            getattr(obj, self.id_field): obj for obj in
            self.model.query.filter(
                getattr(self.model, self.id_field).in_(entity_ids))
        }

    def on_value(self, entity_id, value):
        if self.commit:
            db = current_app.extensions['sqlalchemy'].db
            db.session.add(value)
            db.session.commit()

    def on_exception(self, entity_id, exc):
        pass

    def _worker(self, entity_id):
        raise NotImplementedError()


class LockedFactoryDict(UserDict):
    def __init__(self, factory=None, timeout=None):
        self._factory = factory
        self.timeout = timeout
        super().__init__()

    def __getitem__(self, key):
        if key not in self.data:
            self[key] = lambda: self.factory(key)
        return self.get(key)

    def get(self, key):
        if key in self.data:
            if isinstance(self.data[key], BoundedSemaphore):
                self.data[key].wait(self.timeout)
                assert not isinstance(self.data[key], BoundedSemaphore)
            return self.data[key]

    def __setitem__(self, key, factory):
        if not callable(factory):
            raise ValueError('value %s should be callable' % factory)
        lock = self.data[key] = BoundedSemaphore()
        lock.acquire()
        self.data[key] = factory()
        assert not isinstance(self.data[key], BoundedSemaphore)
        lock.release()

    def factory(self, key):
        if self._factory is not None:
            return self._factory(key)
        raise NotImplementedError()


class Semaphore(Semaphore):
    # NOTE: looks like it's already implemented in newer gevent versions
    """Extends gevent.lock.Semaphore with context."""
    def __enter__(self):
        self.acquire()

    def __exit__(self, *args, **kwargs):
        self.release()
