import pathlib
from setuptools import setup, find_packages

setup(
    name="pymssql-utils",
    version="0.0.1",
    description="A lightweight module that wraps and extends the PyMSSQL library.",
    long_description=(pathlib.Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/invokermain/pymssql-utils",
    author="Tim OSullivan",
    author_email="timothyj725@googlemail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    packages=find_packages(),
    python_requires=">=3.6",
    include_package_data=True,
    install_requires=["pymssql", "python-dateutil==2.8.*"],
)
