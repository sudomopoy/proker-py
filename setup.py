from setuptools import setup, find_packages

setup(
    name='proker',
    version='0.1.2',
    packages=find_packages(where="src"),
    include_package_data=True,
    package_dir={"": "src"},
)
