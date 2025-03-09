from setuptools import find_packages, setup

setup(
    name="myapp",
    packages=find_packages(),
    install_requires=["dagster", "dagster-cloud", "duckdb", "dagster-duckdb", "requests"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
