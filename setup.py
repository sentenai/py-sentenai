
from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='sentenai',
    version='0.1.0',
    description='Client library for Sentenai',
    long_description=long_description,
    url='https://github.com/sentenai/py-sentenai',

    author='Brendan Kohler',
    author_email='brendan@sentenai.com',

    license='BSD',

    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',
        'Topic :: Database :: Internet',

        'License :: OSI Approved :: BSD License',

        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='sentenai cloud sensor database',
    packages=['sentenai'],

    install_requires=['dateutil', 'gevent', 'grequests', 'pandas', 'pytz', 'requests'],
    extras_require={},
    package_data={},
    data_files=[],
    entry_points={},
)

