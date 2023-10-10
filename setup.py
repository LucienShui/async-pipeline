from os import path

from setuptools import setup

from __version__ import __name__, __version__, __author__, __author_email__, __url__, __description__

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as file:
    long_description = file.read()

with open(path.join(this_directory, 'requirements.txt'), encoding='utf-8') as file:
    requirements = file.read().split('\n')

setup(
    name=__name__,
    version=__version__,
    author=__author__,
    author_email=__author_email__,
    url=__url__,
    description=__description__,
    install_requires=requirements,
    packages=['async_pipeline'],
    long_description=long_description,
    long_description_content_type='text/markdown'
)
