from setuptools import setup, find_packages

version = '0.1.1'
__author__ = 's723138643'
__email__ = 's723138643@gmail.com'

setup(
    name='acrawler',
    version=version,
    packages=find_packages(),
    author=__author__,
    author_email=__email__,
    keywords='crawler',
    description='a crawler framework based on asyncio',
    url='https://github.com/s723138643/acrawler',
    include_package_data=True,
    zip_safe=False,
    license='MIT'
)
