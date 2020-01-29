[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client)
[![Build Status](https://travis-ci.org/astrolabsoftware/fink-client.svg?branch=master)](https://travis-ci.org/astrolabsoftware/fink-client)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-client/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-client) [![Documentation Status](https://readthedocs.org/projects/fink-broker/badge/?version=latest)](https://fink-broker.readthedocs.io/en/latest/?badge=latest)

# Fink client

`fink-client` is a light package to manipulate catalogs and alerts issued from the [fink broker](https://github.com/astrolabsoftware/fink-broker) programmatically.

## Installation

You need to have Python 3.5+ installed, and `fink_client>=0.2` installed with dependencies:

```bash
# Install fink-client somewhere on your computer
git clone https://github.com/astrolabsoftware/fink-client
cd fink-client
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Then, assuming you are using bash shell, update your `~/.bash_profile` with the path to the library and binaries:

```bash
# Add these lines at the end of your ~/.bash_profile
export FINK_CLIENT_HOME=${PWD}
export PYTHONPATH=${FINK_CLIENT_HOME}:$PYTHONPATH
export PATH=${FINK_CLIENT_HOME}/bin:$PATH
```

Finally source the file to activate the changes:

```bash
source ~/.bash_profile
```

You can also run integration test (docker-compose required):

```bash
bin/fink_client_test.sh
```
If you don't see any error and all the test results are ok, you have installed it correctly.

## Usage

Learn how to use fink-client by following the dedicated [tutorial](https://fink-broker.readthedocs.io/en/latest/tutorials/using-fink-client/).
