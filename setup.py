'''
Created on 25/02/2013

@author: victor
'''
from distutils.core import setup
import os

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='pyScheduler',
    version='0.1.0',
    description='Compendium of 3 different task schedulers with dependency managing.',
    author='Victor Alejandro Gil Sepulveda',
    author_email='victor.gil.sepulveda@gmail.com',
    url='https://github.com/victor-gil-sepulveda/pyScheduler.git',
    packages=[
              'pyscheduler',
              'pyscheduler.test'
    ],
    license = 'LICENSE.txt',
    long_description = read('README.rst')
)
