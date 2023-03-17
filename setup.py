# Copyright (C) 2022 friendli.ai
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

import os
from setuptools import setup, find_packages


def read(fname: str) -> str:
    return open(os.path.join(os.path.dirname(__file__), fname), encoding='utf-8').read()


def read_version() -> str:
    return read("VERSION").strip()


def read_readme() -> str:
    return read("README.md")


COMMON_DEPS = [
    "requests>=2.26.0",
    "tabulate>=0.8.0",
    "websockets>=10.1",
    "PyYaml>=6.0",
    "ruamel.yaml>=0.17.21",
    "typer>=0.4.0",
    "rich>=12.2.0",
    "jsonschema>=4.4.0",
    "boto3==1.22.8",
    "botocore>=1.25.8",
    "tqdm>=4.64.0",
    "azure-mgmt-storage==20.1.0",
    "azure-storage-blob==12.12.0",
    "packaging>=22.0",
    "pathspec>=0.9.0",
    "boto3-stubs==1.26.90",
    "mypy-boto3-s3==1.26.62",
]

TEST_DEPS = [
    "coverage==5.5",
    "pytest==6.2.4",
    "pytest-asyncio==0.15.1",
    "pytest-cov==2.11.1",
    "pytest-benchmark==3.4.1",
    "pytest-lazy-fixture==0.6.3",
    "requests-mock>=1.9.3",
]

DEV_DEPS = [
    "black>=22.8.0",
    "isort==5.10.1",
]

setup(
    name='periflow-cli',
    version=read_version(),
    long_description=read_readme(),
    long_description_content_type='text/markdown',
    description='PeriFlow CLI',
    author='FriendliAI',
    license="Apache License 2.0",
    url="https://github.com/friendliai/periflow-cli",
    packages=find_packages(include=['pfcli', 'pfcli.*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Natural Language :: English",
    ],
    entry_points={
        "console_scripts": [
            "pf = pfcli:app",
        ]
    },
    python_requires='>=3.8',
    include_package_data=True,
    install_requires=COMMON_DEPS,
    extras_require={
        "test": TEST_DEPS,
        "dev": DEV_DEPS,
    }
)
