#!/usr/bin/env python

from distutils.core import setup

setup(name='python-superfastmatch',
      version='0.1',
      description='Python client API for Superfastmatch',
      author='Drew Vogel',
      author_email='dvogel@sunlightfoundation.com',
      packages=['superfastmatch'],
      requires=['stream>=0.8']
     )
