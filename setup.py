from setuptools import setup

__version = '0.1'

INSTALL_REQUIRE = ['requests>=2.20.0']

setup(
    name='index-factory',
    version=__version,
    description='',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/chris-ch/index-factory',
    author='Christophe',
    author_email='chris.perso@gmail.com',
    packages=['indices', 'rebalancing'],
    package_dir={
        'indices': 'src'
    },
    license='Apache',
    download_url='https://github.com/chris-ch/index-factory/archive/{0}.tar.gz'.format(__version),
    install_requires=INSTALL_REQUIRE,
    zip_safe=True
)
