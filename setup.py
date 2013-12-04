#!/usr/bin/env python

from setuptools import setup

setup(name='python-superfastmatch',
      version='0.1',
      description='Python client API for Superfastmatch',
      author='Drew Vogel',
      author_email='dvogel@sunlightfoundation.com',
      packages=['superfastmatch', 'superfastmatch.tools'],
      requires=['stream (>=0.8)', 'progressbar (>=2.3)', 'gevent (>=0.13.7)']
     )
