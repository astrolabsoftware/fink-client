[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=alert_status)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=astrolabsoftware_fink-client&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=astrolabsoftware_fink-client)
[![Build Status](https://travis-ci.org/astrolabsoftware/fink-client.svg?branch=master)](https://travis-ci.org/astrolabsoftware/fink-client)
[![codecov](https://codecov.io/gh/astrolabsoftware/fink-client/branch/master/graph/badge.svg)](https://codecov.io/gh/astrolabsoftware/fink-client) [![Documentation Status](https://readthedocs.org/projects/fink-broker/badge/?version=latest)](https://fink-broker.readthedocs.io/en/latest/?badge=latest)

# Fink client

`fink-client` is a light package to manipulate catalogs and alerts issued from the [fink broker](https://github.com/astrolabsoftware/fink-broker) programmatically.

## Installation

TBD

## Documentation

**Fink's distribution stream**

Fink distributes alerts via kafka topics based on classification  by cross-match services. See [Fink's re-distribution](https://fink-broker.readthedocs.io/en/latest/user_guide/streaming-out/). For more details on how the alerts are classified, see Fink's tutorial on [processing alerts](https://fink-broker.readthedocs.io/en/latest/tutorials/processing_alerts/).

You can connect to one or more of these topics using fink-client's APIs and receive Fink's stream of alerts.
To obtain security credentials for API access and authorization on kafka topics contact Fink's admins.

**Connecting to Fink's stream of alerts**

Once you have the security credentials for accessing the API, import and instantiate an `AlertConsumer`.

```python
from fink_client.consumer import AlertConsumer

consumer = AlertConsumer{
    topics = ["RRlyr", "AMHer"],
    username = "********"
    password = "********"
    group_id = "client_group"
}
```
A single alert can be received using the `poll()` method of `AlertConsumer`. The alerts are received as tuple of (topic, alert) of type (str, dict).

```python
topic, alert = consumer.poll(timeout=5)

if alert is not None:
    print("topic: ", topic)
    for key, value in alert.items():
        print("key: {}\t value: {}".format(key, value))
else:
    print(f"no alerts received in {timeout} seconds")
```

Multiple alerts can be received using the `consume()` method. The method returns a list of tuples (topic, alert) with maximum `num_alerts` number of alerts.
```python
alerts = counsumer.consume(num_alerts=10, timeout=30)

for topic, alert in alerts:
    print("topic: ", topic)
    print("alert:\n", alert)
```
To consume the alerts in real time one can use the `poll` method in a loop:
```python
# receive alerts in a loop
while True:
    topic, alert = consumer.poll(2)
    print("topic: {}, alert:\n {}".format(topic, alert))
```
Make sure you close the connection, or you can also use the context manager:
```python
# Close the connection explicitly
consumer = AlertConsumer(topics, config)
try:
    topic, alert = consumer.poll(5)
    print("topic: {}, alert:\n {}".format(topic, alert))
finally:
    consumer.close()

# Or use context manager
with AlertConsumer(topics, config) as consumer:
    topic, alert = consumer.poll(5)
    print("topic: {}, alert:\n {}".format(topic, alert))
```
