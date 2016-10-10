from setuptools import setup, find_packages
from distutils.core import Extension

README = 'README.md'

with open('requirements.txt') as f:
    requirements = f.readlines()


def long_desc():
    try:
        import pypandoc
    except ImportError:
        with open(README) as f:
            return f.read()
    else:
        return pypandoc.convert(README, 'rst')

setup(
    name='aionanomsg',
    version='1.1',
    ext_modules=[
        Extension('aionanomsg._nanomsg',
                  sources=['aionanomsg/_nanomsg.c'],
                  libraries=['nanomsg'],
                  library_dirs=['/usr/local/lib'],
        ),
    ],
    description='Asyncio lib for nanomsg (with c bindings)',
    author='Justin Mayfield',
    author_email='tooker@gmail.com',
    url='https://github.com/mayfield/aionanomsg/',
    license='MIT',
    long_description=long_desc(),
    packages=find_packages(),
    test_suite='test',
    install_requires=requirements,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
    ]
)
