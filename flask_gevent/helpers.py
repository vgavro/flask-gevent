from collections import UserDict

from flask import current_app
from werkzeug.utils import cached_property
from gevent import Timeout, joinall
from gevent.lock import Semaphore, BoundedSemaphore

from .utils import repr_pool_status


def __link_greenlet(rv):
    def __link_greenlet(entity_id):
        def ___link_greenlet(greenlet):
            if greenlet.successfull():
                if greenlet.value is not None:
                    rv[entity_id] = greenlet.value
            else:
                rv[entity_id] = greenlet.exception
        return ___link_greenlet
    return __link_greenlet


class EntityBulkProcessor:
    # __call__ returns (data, errors), which are mappings by entity_id
    # if join_timeout expires and worker not finished, no entity_id in data or errors
    # i join_raise=True and join_timeout expires, RuntimeError is raised.
    # worker may:
    #   return value - on_value is triggered, data[entity_id] = value
    #   return None - on_value is not triggered, no entity_id in data or errors
    #   return Exception - on_exception is triggered, no traceback logged
    #   raise Exception - on_exception is triggered, traceback logged

    def __init__(self, pool, update_timeout=10, join_timeout=30,
                 join_raise=False, worker=None, logger=None):
        self.pool = pool
        self.update_timeout = update_timeout
        self.join_timeout = join_timeout
        self.join_raise = join_raise
        if worker:
            self._worker = worker
        self._logger = logger

        self.workers = {}

    @cached_property
    def logger(self):
        return self._logger or current_app.logger

    def __call__(self, *entity_ids, update=True, join=False, join_raise=None):
        entity_ids = set(entity_ids)
        update_timeout = self.update_timeout if update is True else int(update)
        join_timeout = self.join_timeout if join is True else int(join)
        join_raise = self.join_raise if join_raise is None else join_raise

        if update:
            timeout = Timeout.start_new(update_timeout)
        rv = {}
        try:
            workers = self._get_or_update(
                entity_ids, update,
                link=__link_greenlet(rv) if join_timeout else None)
        except Timeout:
            self.logger.warning('Update timeout: %s', entity_ids)
            raise
        else:
            self.logger.debug(
                'Processing: rv=%s spawned=%s pool=%s',
                set(rv.keys()) or '{}',
                [(w.args and w.args[0]) for w in workers],
                repr_pool_status(self.pool),
            )
            timeout.cancel()  # TODO: api?

        if workers and join:
            finished_workers = joinall(workers, timeout=join_timeout)
            if len(workers) != len(finished_workers):
                # w.args[0] is available only for not ready workers,
                # it's cleared otherwise
                entity_ids_ = [w.args[0] for w
                               in set(workers).difference(finished_workers)]
                self.logger.warning(
                    'Join timeout: %d, not finished workers: %s',
                    join_timeout, entity_ids_)
                if join_raise:
                    raise RuntimeError('Join timeout exceeded', entity_ids_)
            # Not needed with link
            # rv_, _ = self._get_or_update(set(entity_ids) - set(rv), update=False)
            # rv.update(rv_)

        data, errors = {}, {}
        for entity_id, value in rv.items():
            if isinstance(value, Exception):
                errors[entity_id] = value
            else:
                data[entity_id] = value
        return data, errors

    def _get_or_update(self, entity_ids, update, link=None):
        rv, workers = self.getter(entity_ids), []
        if update:
            for entity_id in entity_ids.difference(rv.keys()):
                workers.append(self.get_or_create_worker(entity_id, link))
        return rv, workers

    def get_or_create_worker(self, entity_id, link=None):
        if entity_id not in self.workers:
            self.workers[entity_id] = self.pool.spawn(self.worker, entity_id)
        if link:
            self.workers[entity_id].link(lambda: link(entity_id))
        return self.workers[entity_id]

    def worker(self, entity_id):
        self.logger.debug('Starting worker: %s', entity_id)
        try:
            rv = self._worker(entity_id)
        except Exception as exc:
            self.logger.exception('Worker failed: %s %r', entity_id, exc)
            self.on_exception(entity_id, exc)
        except BaseException as exc:
            self.logger.debug('Worker failed: %s %r', entity_id, exc)
            raise
        else:
            if isinstance(rv, Exception):
                # Allowing returning error not to print traceback
                self.logger.warning('Worker failed: %s %r', entity_id, rv)
                self.on_exception(entity_id, rv)
            elif rv is not None:
                self.on_value(entity_id, rv)
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


class CacheEntityBulkProcessor:
    def __init__(self, pool, cache, cache_key, cache_timeout, cache_exception_timeout,
                 **kwargs):
        self.cache = cache
        assert '{}' in cache_key, 'Cache key should have format placeholder'
        self.cache_key = cache_key
        self.cache_timeout = cache_timeout
        self.cache_exception_timeout = cache_exception_timeout
        super().__init__(pool, **kwargs)

    def getter(self, entity_ids):
        rv = {}
        for entity_id in entity_ids:
            data = self.cache.get(self.cache_key.format(entity_id))
            if data is not None:
                rv[entity_id] = data
        return rv

    def on_value(self, entity_id, value):
        self.cache.set(self.cache_key.format(entity_id), value, self.cache_timeout)

    def on_exception(self, entity_id, exc):
        if self.cache_exception_timeout:
            self.cache.set(self.cache_key.format(entity_id), exc,
                           self.cache_exception_timeout)

    def _worker(self, entity_id):
        raise NotImplementedError()


class SqlAlchemyEntityBulkProcessor:
    def __init__(self, pool, model, id_field='id', **kwargs):
        self.model = model
        self.id_field = id_field
        super().__init__(pool, **kwargs)

    def getter(self, entity_ids):
        return {
            getattr(obj, self.id_field): obj for obj in
            self.model.query.filter(
                getattr(self.model, self.id_field).in_(entity_ids))
        }

    def on_value(self, entity_id, value):
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
    # TODO: looks like it's already implemented in newer gevent versions
    """Extends gevent.lock.Semaphore with context."""
    def __enter__(self):
        self.acquire()

    def __exit__(self, *args, **kwargs):
        self.release()
