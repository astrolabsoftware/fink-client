[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client)
[![Build Status](https://travis-ci.org/astrolabsoftware/fink-client.svg?branch=master)](https://travis-ci.org/astrolabsoftware/fink-client)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-client/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-client) [![Documentation Status](https://readthedocs.org/projects/fink-broker/badge/?version=latest)](https://fink-broker.readthedocs.io/en/latest/?badge=latest)

# Fink client

`fink-client` is a light package to manipulate catalogs and alerts issued from the [fink broker](https://github.com/astrolabsoftware/fink-broker) programmatically.

## Installation

Clone the GitHub repository
```bash
git clone https://github.com/astrolabsoftware/fink-client.git
cd fink-client && export FINK_CLIENT_HOME=$(PWD) >> ~/.bash_profile
```
The above expression will place the environment variable for `$FINK_CLIENT_HOME`
into your `~/.bash_profile` such that this variable should not be required to be set again.

Install `fink_client` dependencies
```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Run integration test to verify the working.
```bash
bin/fink_client_test.sh
```
If you don't see any error and all the test results are ok, you have installed it correctly.

## Usage

Learn how to use fink-client by following the dedicated [tutorial]().
