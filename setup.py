from setuptools import setup, find_packages

requires = [
    # core
    'requests[socks]>=2.18.4',
    'pysocks>=1.6.8',
    'gevent',
    'netaddr',  # to allow/block ips to proxy

    # proxy fetcher utils
    'pycountry',
    'pycountry-convert>=0.6',
    'pytimeparse',
    'lxml>=3.7.3',
    'js2py',
    'cssselect>=1.0.1',  # lxml requirement for css selectors

    # cli
    'click',
    'pyyaml',

    # development
    'ipython',
    'pdbpp',
    'coloredlogs',

    # tests
    'requests-mock',
    'pytest>=1.3',
    'pytest-cov',
    'pytest-flake8',

    # for memory leak debug
    # 'mem_top',  # see /mem_top on superproxy
    # 'pillow',  # dozer requirement
    # 'dozer',  # see /_dozer on superproxy
]

setup(
    name='Flask-Gevent',
    version='0.0.1',
    description='http://github.com/vgavro/flask-gevent',
    long_description='http://github.com/vgavro/flask-gevent',
    license='BSD',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    author='Victor Gavro',
    author_email='vgavro@gmail.com',
    url='http://github.com/vgavro/flask-gevent',
    keywords='',
    packages=find_packages(),
    install_requires=requires,
)
