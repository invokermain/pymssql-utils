import pathlib
from setuptools import setup, find_packages

setup(
    name="pymssql-utils",
    version="0.0.5",
    description="A lightweight module that wraps and extends the PyMSSQL library.",
    long_description=(pathlib.Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/invokermain/pymssql-utils",
    author="Tim OSullivan",
    author_email="tim@lanster.dev",
    license="GNU LGPLv2.1",
    classifiers=[
        "License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Database",
        "Typing :: Typed",
    ],
    packages=find_packages(),
    python_requires=">=3.6",
    include_package_data=True,
    install_requires=["pymssql==2.1.*", "python-dateutil==2.8.*", "orjson==3.4.*"],
)
