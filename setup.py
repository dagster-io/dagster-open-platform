from setuptools import find_packages, setup

setup(
    name="purina_open",
    packages=find_packages(exclude=["purina_open_tests"]),
    install_requires=["dagster"],
    extras_require={
        "dev": ["dagster-webserver", "pytest"],
        "tests": ["pytest"],
    },
)
