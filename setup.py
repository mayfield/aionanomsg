from setuptools import setup, find_packages

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
    version='1',
    description='Asyncio lib for nanomsg (using nnpy)',
    author='Justin Mayfield',
    author_email='tooker@gmail.com',
    url='https://github.com/mayfield/aionanomsg/',
    license='MIT',
    long_description=long_desc(),
    packages=find_packages(),
    test_suite='test',
    install_requires=requirements,
    # requirements refs a fake version of nnpy that's not deployed yet.
    # make that fake version available by specific commit here.
    dependency_links=['http://github.com/nanomsg/nnpy/tarball/'
                      '5269d2621d420a0623a5cdd61d56baf3d98cb8e9'
                      '#egg=nnpy-1.2.0.999'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
    ]
)
