# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

def load_readme() -> str:
    with open('README.md') as f:
        readme = f.read()

def load_license() -> str:
    with open('LICENSE') as f:
        license = f.read()

setup(
    name='tempered',
    version='0.0.1',
    description='Web API download manager with rate limiting',
    long_description=load_readme(),
    author='Tillmann Radmer',
    author_email='tillmann.radmer@gmail.com',
    url='https://github.com/radmerti/tempered',
    license=load_license(),
    packages=find_packages(exclude=('tests', 'docs'))
)

