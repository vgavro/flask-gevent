from setuptools import setup, find_packages

requires = [
    'flask',
    'gevent',
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
