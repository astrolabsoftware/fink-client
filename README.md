[![pypi](https://img.shields.io/pypi/v/fink-client.svg)](https://pypi.python.org/pypi/fink-client) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client)
[![Build Status](https://travis-ci.org/astrolabsoftware/fink-client.svg?branch=master)](https://travis-ci.org/astrolabsoftware/fink-client)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-client/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-client) [![Documentation Status](https://readthedocs.org/projects/fink-broker/badge/?version=latest)](https://fink-broker.readthedocs.io/en/latest/?badge=latest)

# Fink client

`fink-client` is a light package to manipulate catalogs and alerts issued from the [fink broker](https://github.com/astrolabsoftware/fink-broker) programmatically.

## Installation

`fink_client` requires a version of Python 3.5+. To install it, just run

```bash
pip install fink-client
```

## Registration

In order to connect and poll alerts from Fink, you need to get your credentials:

1. Subscribe to one or more Fink streams by filling this [form](https://forms.gle/2td4jysT4e9pkf889).
2. After filling the form, we will send your credentials. Register them on your laptop by simply running:
  ```
  fink_client_register -username <USERNAME> -group_id <GROUP_ID> ...
  ```

## Usage

Once you have your credentials, you are ready to poll streams! 

```bash
fink_consumer -h
usage: fink_consumer [-h] [--display] [-limit LIMIT] [--available_topics]
                     [--save] [-outdir OUTDIR]

Kafka consumer to listen and archive Fink streams

optional arguments:
  -h, --help          show this help message and exit
  --display           If specified, print on screen information about incoming
                      alert.
  -limit LIMIT        If specified, download only `limit` alerts. Default is
                      None.
  --available_topics  If specified, print on screen information about
                      available topics.
  --save              If specified, save alert data on disk (Avro). See also
                      -outdir.
  -outdir OUTDIR      Folder to store incoming alerts if --save is set. It
                      must exist.
```

You can also look at an alert on the disk:

```bash
fink_alert_viewer -h
usage: fink_alert_viewer [-h] [-filename FILENAME]

Display cutouts and lightcurve from a ZTF alert

optional arguments:
  -h, --help          show this help message and exit
  -filename FILENAME  Path to an alert data file (avro format)
```

Learn how to use fink-client by following the dedicated [tutorial](https://github.com/astrolabsoftware/fink-client-tutorial). It should not take long to learn it!
