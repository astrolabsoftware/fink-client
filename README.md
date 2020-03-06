[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client)
[![Build Status](https://travis-ci.org/astrolabsoftware/fink-client.svg?branch=master)](https://travis-ci.org/astrolabsoftware/fink-client)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-client/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-client) [![Documentation Status](https://readthedocs.org/projects/fink-broker/badge/?version=latest)](https://fink-broker.readthedocs.io/en/latest/?badge=latest)

# Fink client

`fink-client` is a light package to manipulate catalogs and alerts issued from the [fink broker](https://github.com/astrolabsoftware/fink-broker) programmatically.

## Installation

`fink_client` requires a version of Python 3.5+. 

To install this module run the following commands:

```bash
# Install fink-client somewhere on your computer
git clone https://github.com/astrolabsoftware/fink-client
cd fink-client
pip install --upgrade pip setuptools wheel
pip install .
```

For testing purposes, update your `~/.bash_profile` (assuming you are using bash shell) with the `FINK_CLIENT_HOME` environment variable:

```bash
echo "export FINK_CLIENT_HOME=${PWD}" >> ~/.bash_profile
source ~/.bash_profile
# and run the integration tests (docker-compose required)
fink_client_test.sh
```

If you don't see any error and all the test results are ok, you have installed it correctly.

## Usage

Learn how to use fink-client by following the dedicated [tutorial](https://github.com/astrolabsoftware/fink-client-tutorial).
