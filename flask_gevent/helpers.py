from collections import UserDict

from flask import current_app
from werkzeug.utils import cached_property
from gevent import Timeout, joinall
from gevent.lock import Semaphore, BoundedSemaphore

from .utils import repr_pool_status


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


class CachedBulkProcessor:
    def __init__(self, pool, cache, cache_key, cache_timeout, cache_fail_timeout,
                 update_timeout=10, join_timeout=30, join_raise=False,
                 worker=None, logger=None):
        self.pool = pool
        self.cache = cache
        assert '{}' in cache_key, 'Cache key should have format placeholder'
        self.cache_key = cache_key
        self.cache_timeout = cache_timeout
        self.cache_fail_timeout = cache_fail_timeout
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
        try:
            rv, workers = self._get_or_update(entity_ids, update)
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
                    raise RuntimeError('Join timeout exceeded', len(entity_ids_))
            rv_, _ = self._get_or_update(set(entity_ids) - set(rv), update=False)
            rv.update(rv_)
        return rv

    def _get_or_update(self, entity_ids, update):
        rv, workers = {}, []
        for entity_id in entity_ids:
            data = self.cache.get(self.cache_key.format(entity_id))
            if data:
                rv[entity_id] = data
            elif update:
                workers.append(self.get_or_create_worker(entity_id))
        return rv, workers

    def get_or_create_worker(self, entity_id):
        if entity_id not in self.workers:
            self.workers[entity_id] = self.pool.spawn(self.worker, entity_id)
        return self.workers[entity_id]

    def worker(self, entity_id):
        self.logger.debug('Starting worker: %s', entity_id)
        try:
            rv = self._worker(entity_id)
        except Exception as exc:
            self.logger.exception('Worker failed: %s %r', entity_id, exc)
            self.on_fail(entity_id, exc)
        except BaseException as exc:
            self.logger.debug('Worker failed: %s %r', entity_id, exc)
            raise
        else:
            self.cache.set(self.cache_key.format(entity_id), rv, self.cache_timeout)
        finally:
            del self.workers[entity_id]

    def on_fail(self, entity_id, exc):
        if self.cache_fail_timeout:
            self.cache.set(self.cache_key.format(entity_id), exc,
                           self.cache_fail_timeout)

    def _worker(self, entity_id):
        raise NotImplementedError()


class Semaphore(Semaphore):
    # TODO: looks like it's already implemented in newer gevent versions
    """Extends gevent.lock.Semaphore with context."""
    def __enter__(self):
        self.acquire()

    def __exit__(self, *args, **kwargs):
        self.release()
