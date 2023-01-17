# Copyright 2019-2023 AstroLab Software
# Author: Julien Peloton
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
import setuptools
import fink_client

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = []
with open("requirements.txt", "r") as fr:
    requirements = fr.read().splitlines()

setuptools.setup(
    name="fink-client",
    version=fink_client.__version__,
    author="AstroLab Software",
    author_email="peloton@lal.in2p3.fr",
    description="Light-weight client to manipulate alerts from Fink",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://fink-broker.readthedocs.io/en/latest/",
    install_requires=requirements,
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            'fink_alert_viewer=fink_client.scripts.fink_alert_viewer:main',
            'fink_consumer=fink_client.scripts.fink_consumer:main',
            'fink_client_register=fink_client.scripts.fink_client_register:main',
            'fink_datatransfer=fink_client.scripts.fink_datatransfer:main'
        ]
    },
    scripts=['bin/fink_client_test.sh'],
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Unix",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
    ],
    project_urls={
        'Documentation': "https://fink-broker.readthedocs.io/en/latest/",
        'Source': 'https://github.com/astrolabsoftware/fink-client'
    },
)
