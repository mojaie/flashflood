
from codecs import open
from os import path
from setuptools import setup, find_packages


here = path.abspath(path.dirname(__file__))
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    name="Kiwiii-server",
    version="0.7.0",
    description="HTTP API server library for chemical database administration and computation",
    long_description=long_description,
    url="https://github.com/mojaie/kiwiii-server",
    author="Seiji Matsuoka",
    author_email="mojaie@aol.com",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Chemistry",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6"
    ],
    keywords="drug-discovery cheminformatics api-server",
    packages=find_packages(exclude=["kiwiii.test*"]),
    python_requires=">=3.5",
    install_requires=[
        "chorus", "tornado", "xlsxwriter", "pandas", "python-louvain"
    ]
)
