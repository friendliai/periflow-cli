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


def read(fname):
    """
    Args:
        fname:
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def read_version():
    return read("VERSION").strip()


COMMON_DEPS = [
    "requests>=2.26.0",
    "tabulate>=0.8.0",
    "websockets>=10.1",
    "PyYaml>=6.0",
    "typer>=0.4.0",
    "boto3>=1.20.*",
    "botocore>=1.23.*",
    "azure-mgmt-storage>=19.0.*",
    "azure-storage-blob>=12.9.*"
]

TEST_DEPS = [
    "coverage==5.5",
    "pytest==6.2.4",
    "pytest-asyncio==0.15.1",
    "pytest-cov==2.11.1",
    "pytest-benchmark==3.4.1",
    "pytest-lazy-fixture==0.6.3",
]

setup(
    name='periflow-cli',
    version=read_version(),
    author='FriendliAI',
    license="Apache License 2.0",
    url="https://github.com/friendliai/periflow-cli",
    description="PeriFlow Cli",
    packages=find_packages(where='src'),
    package_dir={'':'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Natural Language :: English",
    ],
    entry_points={
        "console_scripts": [
            "pf=pfcli.__main__:main",
        ]
    },
    include_package_data=True,
    install_requires=COMMON_DEPS + TEST_DEPS
)
