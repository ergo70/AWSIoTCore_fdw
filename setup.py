from setuptools import setup
import os 


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='AWSIoTCore_fdw',
    version='1.0.0',
    author='Ernst-Georg Schmid',
    author_email='pgchem@tuschehund.de',
    packages=['AWSIoTCore_fdw'],
    url='https://github.com/ergo70/AWSIoTCore_fdw',
    license='LICENSE.txt',
    description='PostgreSQL Foreign Data Wrapper for the AWS IoT Core',
    install_requires=["boto3"],
)
