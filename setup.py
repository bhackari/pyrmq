# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='pyrmq',
    version='0.1.0',
    description='Port of adjust/rmq in Python',
    long_description=readme,
    author='Mirco De Zorzi',
    author_email='mircodezorzi@protonmail.com',
    url='https://github.com/bhackari/pyrmq',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)
