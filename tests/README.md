# Manipulating fake streams

Requirements:

* Docker & docker compose installed on your machine
* fink-client version 2.7+ cloned somewhere

For test purposes, you can produce fake streams, and consume them. Walk to `fink-client`, and enter the `tests` folder. Alerts will be produced locally on the topic `test_stream`. Generate credentials to handle this stream:

```bash
# Fake credentials
fink_client_register -username test -password None \
  -servers 'localhost:9093, localhost:9094, localhost:9095' \
  -mytopics test_stream -group_id test_group -maxtimeout 10 --tmp
```

Then, make sure Docker is running, and launch the Kafka server:

```bash
docker-compose -p broker_test -f docker-compose-kafka.yml up -d
```


produce a stream of data using the `produce_fake.py` script:

```bash
python produce_fake.py
```

You might see an error message once, that you can ignore safely

```
%3|1639723497.610|FAIL|rdkafka#consumer-1| [thrd:localhost:9094/bootstrap]:
localhost:9094/bootstrap: Connect to ipv4#127.0.0.1:9094 failed: Connection
refused (after 1ms in state CONNECT)
```

And consume the stream using:

```bash
fink_consumer --display -limit 2 -schema ../schemas/schema_test.avsc
+----------------------------------+---------------------+-------------+--------------+-----------------+--------------------+
|         Emitted at (UTC)         |  Received at (UTC)  |    Topic    |   objectId   |     Simbad      |     Magnitude      |
+----------------------------------+---------------------+-------------+--------------+-----------------+--------------------+
| 2021-11-22 08:33:05.999045+00:00 | 2022-02-09 10:32:51 | test_stream | ZTF17aabvtfi | Candidate_TTau* | 18.799415588378906 |
+----------------------------------+---------------------+-------------+--------------+-----------------+--------------------+
+----------------------------------+---------------------+-------------+--------------+----------------+--------------------+
|         Emitted at (UTC)         |  Received at (UTC)  |    Topic    |   objectId   |     Simbad     |     Magnitude      |
+----------------------------------+---------------------+-------------+--------------+----------------+--------------------+
| 2021-11-22 02:37:20.999995+00:00 | 2022-02-09 10:32:51 | test_stream | ZTF18abwvktq | Candidate_AGB* | 16.502267837524414 |
+----------------------------------+---------------------+-------------+--------------+----------------+--------------------+
```

et voilà! Once you have consumed all the 320 alerts, you can relaunch `produce_fake.py ` as many times as you want to get new alerts (the same 320 alerts sent each time).

## Generate schema for new alert data

We distribute the corresponding schema for the tests, but in case you have other data, here is the procedure. Fink internally stores alerts data as Parquet files. So you need first to convert to Avro and extract the schema:

```python
import json
from fink_broker.avroUtils import readschemafromavrofile

# Load Parquet files containing alert data
df = spark.read.format('parquet').load('sample.parquet/')

# Convert to Avro on disk
df.write.format("avro").option('compression', 'uncompressed').save("sample.avro")

# Extract schema
avro_schema = readschemafromavrofile('sample.avro/one_of_the_file.avro')

# Save the schema on disk
with open('schema.avsc', 'w') as f:
	json.dump(avro_schema, f, indent=2)
```

You can then use `sample.avro/` and `schema.avsc` to produce alerts.
