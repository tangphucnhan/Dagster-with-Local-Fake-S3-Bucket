from setuptools import find_packages, setup

setup(
    name="cafebills",
    packages=find_packages(exclude=["cafebills_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
