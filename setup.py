from setuptools import find_packages, setup

setup(
    name="dagster-open-platform",
    packages=find_packages(exclude=["dagster_open_platform_tests"]),
    install_requires=[
        "dbt-snowflake~=1.7",
        "dagster",
        "dagster-snowflake",
        "dagster-slack",
        "dagster-pandas",
        "dagster-hightouch",
        "dagster-gcp",
        "dagster-dbt",
        "dagster-cloud",
        "dagster-embedded-elt",
        "sling",
        "gql[requests]",
        "dlt[snowflake,duckdb]",
        "pydantic>2",
        "pyyaml",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest", "click"],
        "tests": ["pytest", "responses"],
    },
)
