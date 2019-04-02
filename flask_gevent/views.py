from flask import current_app

from .utils import repr_pool_status


def get_app_status():
    rv = {
        'pools': {
            name: repr_pool_status(pool) for name, pool
            in current_app.extensions['gevent'].pools.items()
        },
    }
    if 'sqlalchemy' in current_app.extensions:
        rv['sqlalchemy'] = \
            current_app.extensions['sqlalchemy'].db.session.bind.pool.status()
    return rv
