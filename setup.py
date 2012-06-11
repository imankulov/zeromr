#!/usr/bin/env python
# -*- coding: utf8 -*-
from setuptools import setup, find_packages

import os, sys
reload(sys).setdefaultencoding("UTF-8")

def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname)).read()
    except:
        return ''

setup(
    name='zeromr',
    version='0.1',
    author='Roman Imankulov',
    author_email='info@netangels.ru',
    packages=find_packages(),
    scripts=[
        'bin/shuffler',
        'bin/worker',
        'bin/sender',
    ],
    url='http://github.com/imankulov/zeromr',
    license = 'BSD License',
    description = u'ZeroMQ based map/reduce framework',
    long_description = read('README.rst'),
    install_requires = [
        'pyzmq',
        'redis',
    ],
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
    ),
)
