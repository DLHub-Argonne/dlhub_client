from setuptools import setup

setup(
    name='dlhub_client',
    version='0.0.1',
    packages=['dlhub_client'],
    description='DLHub Python client',
    long_description=("DLHub Client is the python package for DLHub."
                     " DLHub Client allows users to easily access"
                     " machine learning and deep learning models"
                     " to facilitate scientific discovery."),
    install_requires=[
        "pandas",
        "requests>=2.18.4",
        "ipywidgets"
    ],
    python_requires=">=3.4",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering"
    ],
    keywords=[
        "DLHub",
        "Data and Learning Hub for Science",
        "machine learning",
        "deep learning",
        "data publication",
        "reproducibility",
    ],
    license="Apache License, Version 2.0",
    url="https://github.com/DLHub-Argonne/dlhub_client"
)
