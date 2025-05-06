from setuptools import setup, find_packages

setup(
    name="maciej_bep",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pymongo",
        "ijson"
    ],
)