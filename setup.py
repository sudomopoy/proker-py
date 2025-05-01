from setuptools import setup, find_packages

setup(
    name="proker",
    version="0.1.4",
    packages=find_packages(where="src"),
    include_package_data=True,
    package_dir={"": "src"},
    description="a dependency inversion python package for using rabbit-mq and kafka",
    author="mopoy",
    author_email="mopoy.code@gmail.com",
    url="https://github.com/sudomopoy/proker-py",  # Optional
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    dependency_links=[
        "https://pypi.org/project/pika/",
        "https://pypi.org/project/kafka-python/",
    ],
    install_requires=["pika>=1.1.0", "kafka-python>=2.0.2", "protobuf>=5.29.3"],
)
