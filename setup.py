from setuptools import find_packages, setup

setup(
    name="dagster_open_platform",
    packages=find_packages(exclude=["dagster_open_platform_tests"]),
    install_requires=[
        "dbt-snowflake",
        "dagster",
        "dagster-snowflake",
        "dagster-pandas",
        "dagster-gcp",
        "dagster-dbt",
        "dagster-cloud",
        "dagster-slack",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest"],
        "tests": ["pytest"],
    },
)
