def patch_all(psycopg=True, **kwargs):
    import gevent.monkey
    gevent.monkey.patch_all(**kwargs)
    if psycopg:
        try:
            from psycogreen.gevent import patch_psycopg
            patch_psycopg()
        except ImportError:
            pass


def patch_sqlalchemy(db):
    if hasattr(db, 'extensions'):
        if 'sqlalchemy' not in db.extensions:
            raise RuntimeError('sqlalchemy is not initialized on %s' % db)
        db = db.extensions['sqlalchemy'].db
    db.engine.pool._use_threadlocal = True
