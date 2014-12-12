#!/usr/bin/env python
# -*- coding: utf-8 -*-
from distutils.core import setup

#print "setup.py not implemented yet. Contact WideIO"
#exit(1)

#by Sohail. First try.
# git@github.com:wideioltd/mongoapi2sql.git
# https://github.com/wideioltd/mongoapi2sql
#Provides a sqlite3 interface for NuoDB?
setup(
    name='mongoapi2sql',
    version='0.2',
    author='WideIO',
    author_email='hello@wide.io',
    description='Mongo-like API to sqlite3 (sqlite3 driver)',
    keywords='nuodb mongodb sqlite sqlite3 database sql',
    url='https://github.com/wideioltd/mongoapi2sql',
    license='BSD licence, see LICENCE.txt',
    long_description=open('README.md').read(),
    packages=['mongoapi2sql'],
    #package_dir={'': 'mongoapi2sql'},
    #packages=[''],
    #package_dir={'mongoapi2sql': 'mongo'},
    #packages=['mongoapi2sql'],
    #package_dir={'mongoapi2sql': 'mongo'},
)

#glob.glob(os.path.join('mydir', 'subdir', '*.html'))
#os.listdir(os.path.join('mydir', 'subdir'))

#setup(...,
#      ext_package='pkg',
#      ext_modules=[Extension('foo', ['foo.c']),
#                   Extension('subpkg.bar', ['bar.c'])],
#     )


#https://docs.python.org/2/distutils/setupscript.html
