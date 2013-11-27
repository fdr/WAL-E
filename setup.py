#!/usr/bin/env python
import os.path
import sys

# Version file managment scheme and graceful degredation for
# setuptools borrowed and adapted from GitPython.
try:
    from setuptools import setup, find_packages

    # Silence pyflakes
    assert setup
    assert find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

if sys.version_info < (2, 6):
    raise RuntimeError('Python versions < 2.6 are not supported.')


# Utility function to read the contents of short files.
def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()

VERSION = read(os.path.join('wal_e', 'VERSION')).strip()

install_requires = [
    l for l in read('requirements.txt').split('\n')
    if l and not l.startswith('#')]

tests_require = [
    "pytest-capturelog>=0.7",
    "pytest-cov",
    "pytest-flakes",
    "pytest-pep8",
    "pytest-xdist",
    "pytest>=2.2.1",
]

if sys.version_info < (2, 7):
    install_requires.append('argparse>=0.8')

from setuptools.command.test import test as TestCommand

test_args = []
if sys.argv[1] == 'test':
    test_args = sys.argv[2:]
    sys.argv = sys.argv[:2]

class PyTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = test_args
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        self.test_args = ['--pep8', '--flakes', 'tests', 'wal_e'] + self.test_args
        print self.test_args
        errno = pytest.main(self.test_args)
        sys.exit(errno)

setup(
    name="wal-e",
    version=VERSION,
    packages=find_packages(),

    install_requires=install_requires,
    tests_require=tests_require,
    cmdclass = {'test': PyTest},

    # metadata for upload to PyPI
    author="Daniel Farina",
    author_email="daniel@heroku.com",
    description="PostgreSQL WAL-shipping for S3",
    long_description=read('README.rst'),
    classifiers=['Topic :: Database',
                 'Topic :: System :: Archiving',
                 'Topic :: System :: Recovery Tools'],
    platforms=['any'],
    license="BSD",
    keywords=("postgres postgresql database backup archive "
              "archiving s3 aws wal shipping"),
    url="https://github.com/wal-e/wal-e",

    # Include the VERSION file
    package_data={'wal_e': ['VERSION']},

    # install
    entry_points={'console_scripts': ['wal-e=wal_e.cmd:main']})
