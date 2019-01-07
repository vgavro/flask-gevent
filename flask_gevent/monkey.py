def patch_all(psycopg=True, **kwargs):
    import gevent.monkey
    gevent.monkey.patch_all(**kwargs)
    if psycopg:
        try:
            from psycogreen.gevent import patch_psycopg
            patch_psycopg()
        except ImportError:
            pass
