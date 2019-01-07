import sys
import signal

import gevent
import gevent.pool
import gevent.pywsgi


def serve_forever(app, **kwargs):
    def get_config(name, default):
        return kwargs.pop(name, app.config.get('GEVENT_SERVER_%s' % name.upper(), default))

    host, port = get_config('listen', '127.0.0.1:8088').split(':')
    exit_signals = get_config('exit_signals', [signal.SIGTERM, signal.SIGINT])
    pool_size = get_config('pool_size', None)
    spawn = get_config('spawn', gevent.pool.Pool(pool_size))

    # Well, it don't looks like good idea...
    # log = get_config('log', app.logger)
    # error_log = get_config('error_log', app.logger)

    pools, lifecycle = app.extensions['gevent'].pools, app.extensions['gevent'].lifecycle

    if isinstance(spawn, gevent.pool.Pool):
        pools['_server'] = spawn

    server = gevent.pywsgi.WSGIServer((host, int(port)), app,
        spawn=spawn, **kwargs)

    def exit():
        if hasattr(exit, 'exiting'):
            try:
                server.error_log.write('Multiple exit signals received - aborting\n')
            finally:
                return sys.exit('Multiple exit signals received - aborting')
        exit.exiting = True
        server.log.write('Server stopping\n')
        lifecycle.pool.kill(timeout=5)
        server.stop(timeout=5)
        for name, pool in pools.items():
            pool.kill(timeout=5)
        server.log.write('Server stopped gracefully\n')

    [gevent.signal(sig, exit) for sig in exit_signals]

    server.log.write('Server starting on %s:%s\n' % (host, port))
    server.serve_forever()
