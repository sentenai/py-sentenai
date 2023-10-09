from setuptools import setup

setup(
    name='sentenai',
    version='1.6.5.2',
    description='Client library for Sentenai',
    long_description="",
    url='https://github.com/sentenai/py-sentenai',

    author='Sentenai, Inc.',
    author_email='info@sentenai.com',

    license='BSD',

    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',
        'Topic :: Database',

        'License :: OSI Approved :: BSD License',

        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='sentenai cloud sensor database',
    packages=['sentenai', 'sentenai.stream'],

    install_requires=['dateutils', 'pytz', 'requests', 'shapely', 'simplejson', 'numpy', 'treelib', 'tqdm', 'cbor2'],
    extras_require={},
    package_data={},
    data_files=[],
    entry_points={},
)
