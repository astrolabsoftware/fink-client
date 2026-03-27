[![pypi](https://img.shields.io/pypi/v/fink-client.svg)](https://pypi.python.org/pypi/fink-client)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=fink-client&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=fink-client)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=fink-client&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=fink-client)
[![Sentinel](https://github.com/astrolabsoftware/fink-client/workflows/Sentinel/badge.svg)](https://github.com/astrolabsoftware/fink-client/actions?query=workflow%3ASentinel)

# Fink client

`fink-client` is a light package around Apache Kafka tools to manipulate catalogs and alerts issued from the [fink broker](https://github.com/astrolabsoftware/fink-broker) programmatically. It is used in the context of 3 major Fink services: Livestream, Data Transfer, and Xmatch.

## Installation

`fink_client` requires a version of Python 3.9+. You can easily install it via pip:

```bash
pip install fink-client --upgrade
```

## Documentation

Depending on the survey used, you will find documentation at:

| Service | Documentation links |
|-|-|
| Livestream | [LSST](https://doc.lsst.fink-broker.org/services/livestream/) / [ZTF](https://doc.ztf.fink-broker.org/en/latest/services/livestream/) |
| Data Transfer | [LSST](https://doc.lsst.fink-broker.org/services/data_transfer/) / [ZTF](https://doc.ztf.fink-broker.org/en/latest/services/data_transfer/) |
| Xmatch | LSST (to come) / [ZTF](https://doc.ztf.fink-broker.org/en/latest/services/xmatch/) |


## Registration

In order to connect and poll alerts from Fink, you need to get your credentials:

1. Subscribe to one or more Fink streams by filling this [form](https://forms.gle/2td4jysT4e9pkf889).
2. After filling the form, we will send your credentials. Register them on your laptop by simply running:
  ```
  fink_client_register -survey SURVEY -username USERNAME -group_id GROUP_ID -servers SERVERS ...
  ```

Note that `SURVEY` is among `ztf` or `lsst`. In case of doubt, run `fink_client_register -h`. You can also inspect the configuration file on disk:

```bash
cat ~/.finkclient/ztf_credentials.yml
cat ~/.finkclient/lsst_credentials.yml
```

Note for users migration from v9.x to v10.x: your credentials remain valid, but you have to register again via the command line to generate correct credential files.

## Contributing

For development, we recommend the use of a virtual environment:

```bash
git clone https://github.com/astrolabsoftware/fink-client.git
cd fink-client
python -m venv .fc_env
source .fc_env/bin/activate
pip install -r requirements.txt
pip install .
```

Feel free to open a Pull Request for bug fixes or new features.
