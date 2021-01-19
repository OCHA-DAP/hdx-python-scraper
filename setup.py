# -*- coding: utf-8 -*-
from os.path import join

from hdx.utilities import CleanCommand, PackageCommand, PublishCommand
from hdx.utilities.loader import load_file_to_str
from setuptools import setup, find_packages

requirements = ['hdx-python-api>=4.8.7',
                'olefile',
                'pygsheets',
                'regex']

extras_requirements = {'pandas': ['pandas']}

classifiers = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "Natural Language :: English",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

PublishCommand.version = load_file_to_str(join('src', 'hdx', 'scraper', 'version.txt'), strip=True)

setup(
    name='hdx-python-scraper',
    description='HDX Python Scraper Library',
    license='MIT',
    url='https://github.com/OCHA-DAP/hdx-python-scraper',
    version=PublishCommand.version,
    author='Michael Rans',
    author_email='rans@email.com',
    keywords=['HDX', 'API', 'library'],
    long_description=load_file_to_str('README.md'),
    long_description_content_type='text/markdown',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    zip_safe=True,
    classifiers=classifiers,
    install_requires=requirements,
    extras_require=extras_requirements,
    cmdclass={'clean': CleanCommand, 'package': PackageCommand, 'publish': PublishCommand},
)
