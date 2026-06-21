from setuptools import setup, find_packages

setup(
    name="fintrust-risk-platform",
    version="0.1.5",
    packages=find_packages(include=["src", "src.*"]),
    install_requires=["pyyaml"],
)
