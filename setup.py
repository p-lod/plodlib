#!/usr/bin/env python

from distutils.core import setup

setup(name='plodlib',
      version='0.1.2',
      description='Library and CLI for accessing the P-LOD triplestore',
      author='Sebastian Heath',
      author_email='sebastian.heath@nyu.edu',
      url='https://github.com/p-lod/plodlib/',
      packages=['plodlib'],
      install_requires=[
         'pandas',
         'rdflib>=7.0.0',
         'requests'
        ]
     )

