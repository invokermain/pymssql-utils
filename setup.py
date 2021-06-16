import pathlib
from setuptools import setup, find_packages

extra_json = ["orjson>=3.4.0,<4.0.0"]
extra_test = [
    "pytest-mock>=3.6.0,<4.0.0",
    "pandas>=1.0.0,<2.0.0",
    "pytest-dotenv>=0.5.0,<1.0.0",
]
extra_all = extra_json + extra_test

setup(
    name="pymssql-utils",
    version="0.0.16",
    description="pymssql-utils is a small library that wraps pymssql to make your life easier.",
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
    install_requires=[
        "pymssql>=2.1.0,<3.0.0",
        "python-dateutil>=2.8.0,<3.0.0",
    ],
    extras_require={
        "all": extra_all,
        "json": extra_json,
        "test": extra_test,
    },
)
