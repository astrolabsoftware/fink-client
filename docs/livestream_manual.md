# Fink livestream manual

_version 0.1 05/03/2021_

This manual has been tested for `fink-client` version 1.2. Other versions might work. In case of trouble, send us an email (contact@fink-broker.org) or open an issue (https://github.com/astrolabsoftware/fink-client).

## Installation of fink-client

From a terminal, you can install fink-client simply using `pip`:

```
pip install fink-client
```

This should install all necessary dependencies.

## Registering

You first need to register your credentials. Upon installation, run on a terminal:

```bash
# access help using `fink_client_register -h`
fink_client_register \
	-username <USERNAME> \
	-group_id <GROUP_ID> \
	-mytopics <topic1 topic2 etc> \ # space separated if several
	-servers <SERVER> \ # comma separated if several
	-maxtimeout 5 \
	 --verbose
```

where `<USERNAME>`, `<GROUP_ID>`, `topics` and `<SERVER>` have been sent to you privately. By default, the credentials are installed in the home:

```bash
cat ~/.finkclient/credentials.yml
```

## First steps: testing the connection

Processed alerts are stored 1 week on our servers, which means if you forget to poll data, you'll be able to retrieve it up to one week after emission. This also means on your first connection, you will have one week of alert to retrieve. Before you get all of them, let's retrieve the first available one to check the connection. On a terminal, run the following

```bash
# access help using `fink_consumer-h`
fink_consumer --display -limit 1
```

This will download the first available alert, and print some useful information.  The alert schema is automatically downloaded from the GitHub repo. But then the alert is consumed and you'll move to the next alert. Of course, if you want to keep the data, you need to store it. This can be easily done:

```bash
# create a folder to store alerts
mkdir alertDB

# access help using `fink_consumer-h`
fink_consumer --display --save -outdir alertDB -limit 1
```

This will download the next available alert, display some useful information on screen, and save it (avro format) on disk. Then if all works, then you can remove the limit, and let the consumer run for ever!

```bash
# access help using `fink_consumer-h`
fink_consumer --display --save -outdir alertDB
```

## Inspecting alerts

Once alerts are saved, you can open it and explore the content. We wrote a small utility to quickly visualise it:

```bash
# access help using `fink_alert_viewer -h`
# Adapt the filename accordingly
fink_alert_viewer -filename alertDB/ZTF21aancozh.avro
```

of course, you can develop your own tools!

## Going beyond

You can write your own consumer with handler to manipulate alerts upon receival. We give an simple example here. Open your favourite editor, and paste the following lines:

```python
""" Poll the Fink servers only once at a time """
from fink_client.consumer import AlertConsumer
from fink_client.configuration import load_credentials

def poll_single_alert(myconfig, topics) -> None:
    """ Connect to and poll fink servers once.
    
    Parameters
    ----------
    myconfig: dic
    	python dictionnary containing credentials
    topics: list of str
    	List of string with topic names
    """
    maxtimeout = 5

    # Instantiate a consumer
    consumer = AlertConsumer(topics, myconfig)

    # Poll the servers
    topic, alert = consumer.poll(maxtimeout)

    # Analyse output
    if topic is not None:
        print("-" * 65)
        row = [
            alert['timestamp'], topic, alert['objectId'],
            alert['roid'], alert['rfscore'], alert['snn_snia_vs_nonia']
        ]
        print("{:<25}|{:<10}|{:<15}|{}|{:<10}|{:<10}".format(*row))
    else:
        print(
            'No alerts received in the last {} seconds'.format(
                maxtimeout
            )
        )

    # Close the connection to the servers
    consumer.close()


if __name__ == "__main__":
    """ Poll the servers only once at a time """

    # to fill
    myconfig = {
        'username': '',
        'bootstrap.servers': '',
        'group_id': ''
    }

    topics = ['', '']

    poll_single_alert(myconfig, topics)
```

You only need to update the `myconfig` dictionnary with the connection information sent to you privately, and the `topics` list with the topics you requested. Save the file, and in a terminal walk to where the file has been saved and execute it:

```bash
python my_consumer.py
```

You should start to see alert flowing!

## Troubleshooting

In case of trouble, send us an email (contact@fink-broker.org) or open an issue (https://github.com/astrolabsoftware/fink-client). A typical error though would be:

```
Traceback (most recent call last):
  File "/Users/julien/anaconda3/bin/fink_consumer", line 10, in <module>
    sys.exit(main())
  File "/Users/julien/Documents/workspace/myrepos/fink-client/fink_client/scripts/fink_consumer.py", line 92, in main
    topic, alert = consumer.poll(timeout=maxtimeout)
  File "/Users/julien/Documents/workspace/myrepos/fink-client/fink_client/consumer.py", line 94, in poll
    alert = _decode_avro_alert(avro_alert, self._parsed_schema)
  File "/Users/julien/Documents/workspace/myrepos/fink-client/fink_client/avroUtils.py", line 381, in _decode_avro_alert
    return fastavro.schemaless_reader(avro_alert, schema)
  File "fastavro/_read.pyx", line 835, in fastavro._read.schemaless_reader
  File "fastavro/_read.pyx", line 846, in fastavro._read.schemaless_reader
  File "fastavro/_read.pyx", line 561, in fastavro._read._read_data
  File "fastavro/_read.pyx", line 456, in fastavro._read.read_record
  File "fastavro/_read.pyx", line 559, in fastavro._read._read_data
  File "fastavro/_read.pyx", line 431, in fastavro._read.read_union
  File "fastavro/_read.pyx", line 555, in fastavro._read._read_data
  File "fastavro/_read.pyx", line 349, in fastavro._read.read_array
  File "fastavro/_read.pyx", line 561, in fastavro._read._read_data
  File "fastavro/_read.pyx", line 456, in fastavro._read.read_record
  File "fastavro/_read.pyx", line 559, in fastavro._read._read_data
  File "fastavro/_read.pyx", line 405, in fastavro._read.read_union
IndexError: list index out of range
```

This error happens when the schema to decode the alert is not matching the alert content. If you see this happening, just open an issue in the repo and quote the schema version you are using: 

```python
import fink_client
print(fink_client.__schema_version__)
```

we will figure out a solution!