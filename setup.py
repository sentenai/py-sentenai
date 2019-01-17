from setuptools import setup

setup(
    name='sentenai',
    version='0.99.1.3',
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
    packages=['sentenai', 'sentenai.api'],

    install_requires=['dateutils', 'pandas', 'pytz', 'requests', 'shapely', 'virtualtime', 'simplejson', 'scipy'],
    extras_require={},
    package_data={},
    data_files=[],
    entry_points={},
)
