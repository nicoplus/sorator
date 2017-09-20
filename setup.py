# -*- coding: utf-8 -*-

import os
import re
import ast
from setuptools import find_packages
from distutils.core import setup


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'orator/__init__.py')) as f:
    _version_re = re.compile(r'__version__\s+=\s+(.*)')
    version = str(ast.literal_eval(_version_re.search(
        f.read()).group(1)))

with open(os.path.join(here, 'requirements.txt')) as f:
    requirements = f.readlines()

setup_kwargs = dict(
    name='sorator',
    license='MIT',
    version=version,
    description='The Orator ORM provides a simple yet beautiful ActiveRecord implementation.',
    long_description=open('README.rst').read(),
    entry_points={
        'console_scripts': ['orator=orator.commands.application:application.run'],
    },
    author='Sébastien Eustace',
    author_email='sebastien.eustace@gmail.com',
    url='https://github.com/shanbay/orator',
    download_url='https://github.com/shanbay/orator/archive/%s.tar.gz' % version,
    packages=find_packages(exclude=['tests']),
    install_requires=requirements,
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)

setup(**setup_kwargs)
