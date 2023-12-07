from setuptools import setup, find_packages

setup(name="Tan", 
    version="1.0", 
    packages=find_packages(
    where='data'),
    package_dir={"" : "data"},
    python_requires=">=3.8",)

