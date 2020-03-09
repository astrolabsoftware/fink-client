[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client)
[![Build Status](https://travis-ci.org/astrolabsoftware/fink-client.svg?branch=master)](https://travis-ci.org/astrolabsoftware/fink-client)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-client/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-client) [![Documentation Status](https://readthedocs.org/projects/fink-broker/badge/?version=latest)](https://fink-broker.readthedocs.io/en/latest/?badge=latest)

# Fink client

`fink-client` is a light package to manipulate catalogs and alerts issued from the [fink broker](https://github.com/astrolabsoftware/fink-broker) programmatically.

## Installation

`fink_client` requires a version of Python 3.5+. To install it, just run

```bash
pip install fink-client
```

## Usage

In order to connect and poll alerts from Fink, you need to get your credentials:

1. subscribe to one or more Fink streams by filling this [form](https://forms.gle/2td4jysT4e9pkf889). 
2. After filling the form, we will send your credentials. Register them on your laptop by simply running:
  ```
  fink_client_register -username <USERNAME> -group_id <GROUP_ID> ...
  ```

Finally, learn how to use fink-client by following the dedicated [tutorial](https://github.com/astrolabsoftware/fink-client-tutorial).
