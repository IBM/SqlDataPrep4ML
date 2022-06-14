import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="SQL Data Preprocessing",
    version="0.0.1",
    author="IBM Research",
    author_email="p.novotny@ibm.com",
    description="A SQL implementation of Machine Learning preprocessing functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/...",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)