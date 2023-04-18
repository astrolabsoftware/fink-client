# Fink data transfer manual

_date 26/01/2023_

This manual has been tested for `fink-client` version 4.4. In case of trouble, send us an email (contact@fink-broker.org) or open an issue (https://github.com/astrolabsoftware/fink-client).

## Installation of fink-client

From a terminal, you can install fink-client simply using `pip`:

```
pip install fink-client --upgrade
```

This should install all necessary dependencies.

## Registering

You first need to register your credentials. Upon installation, run on a terminal:

```bash
# access help using `fink_client_register -h`
fink_client_register \
	-username <USERNAME> \ # given privately
	-group_id <GROUP_ID> \ # given privately
	-mytopics <topic1 topic2 etc> \ # see https://fink-broker.readthedocs.io/en/latest/topics/
	-servers <SERVER> \ # given privately, comma separated if several
	-maxtimeout 5 \ # in seconds
	 --verbose
```

where `<USERNAME>`, `<GROUP_ID>`, and `<SERVER>` have been sent to you privately when filling this [form](https://forms.gle/2td4jysT4e9pkf889). By default, the credentials are installed in the home:

```bash
cat ~/.finkclient/credentials.yml
```

For the list of available topics, see [https://fink-broker.readthedocs.io/en/latest/topics/](https://fink-broker.readthedocs.io/en/latest/topics/).

## First steps: testing the connection

Processed alerts are stored 1 week on our servers, which means if you forget to poll data, you'll be able to retrieve it up to one week after emission. Before you get all of them, let's retrieve only a few alerts to check the connection. On a terminal, run the following

```bash
# access help using `fink_datatransfer -h`
fink_datatransfer \
    -topic <topic name> \
    -outdir myalertfolder \
    -partitionby finkclass \
    --verbose \
    -limit 20
```

This will download and store the first 20 available alerts, and partition the data according to their `finkclass`:

```bash
myalertfolder/
├── Early SN Ia candidate
├── SN candidate
├── Solar System candidate
└── Unknown
...
```

Note that the alert schema is automatically downloaded from the Kafka servers.

## Downloading, resuming and starting from scratch

Once you are ready, you can poll all alerts in the topic:

```bash
# access help using `fink_datatransfer -h`
fink_datatransfer \
    -topic <topic name> \
    -outdir myalertfolder \
    -partitionby finkclass \
    --verbose
```

You can stop the poll by hitting `CTRL+C` on your keyboard, and resume later. The poll will restart from the last offset, namely you will not have duplicate. In case you want to start polling data from the beginning of the stream, you can use the `--restart_from_beginning` option:

```bash
# Make sure `myalertfolder` is empty or does not
# exist to avoid duplicates.
fink_datatransfer \
    -topic <topic name> \
    -outdir myalertfolder \
    -partitionby finkclass \
    --verbose \
    --restart_from_beginning
```

## Re-using the same queue

In case you

## Reading alerts

Alerts are saved in the Apache Parquet format. Assuming you are using Python, you can easily read them using Pandas:

```python
import pandas as pd

# you can pass the folder name
pdf = pd.read_parquet('myalertfolder')
```

## Troubleshooting

In case of trouble, send us an email (contact@fink-broker.org) or open an issue (https://github.com/astrolabsoftware/fink-client).

### Known bugs

1. Data from 2019/2020/2021 and 2022/2023 are not compatible (different schemas). We will resolve the problem soon, but in the meantime, do not mix data from the two periods in a single query.
2. With version 4.0, you wouldn't have the partitioning column when reading in a dataframe. This has been corrected in 4.1.
3. If you have recurrent timeouts, try to increase the timeout in your configuration file `~/.finkclient/credentials.yml`.
4.
