from setuptools import find_packages, setup

setup(
    name="purina_open",
    packages=find_packages(exclude=["purina_open_tests"]),
    install_requires=[
        "dbt-snowflake",
        "dagster",
        "dagster-snowflake",
        "dagster-pandas",
        "dagster-gcp",
        "dagster-dbt",
        "dagster-cloud",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest"],
        "tests": ["pytest"],
    },
)
