from setuptools import setup

setup(
    name='sentenai',
    version='1.0.1.1',
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

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='sentenai cloud sensor database',
    packages=['sentenai', 'sentenai.stream', 'sentenai.pattern', 'sentenai.view'],

    install_requires=['dateutils', 'pytz', 'requests', 'shapely', 'simplejson', 'numpy'],
    extras_require={},
    package_data={},
    data_files=[],
    entry_points={},
)
