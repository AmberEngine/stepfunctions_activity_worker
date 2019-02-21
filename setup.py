"""Setuptools packaging for stepfunctions_activity_worker."""
from setuptools import setup

install_requires = [
    "boto3<2",
]
tests_require = install_requires + [
    "pytest==3.3.1",
]

setup(
    name="stepfunctions_activity_worker",
    version="1.1.0",
    liscense="MIT",
    url="https://github.com/AmberEngine/stepfunctions_activity_worker",

    description="Activity worker for performing AWS StepFunctions tasks",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",

    author="Amber Engine LLC",
    author_email="developers@amberengine.com",

    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=["pytest-runner>=2.0"],

    packages=["stepfunctions_activity_worker"],
)
