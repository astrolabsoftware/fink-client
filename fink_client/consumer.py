#!/usr/bin/env python
# Copyright 2019-2020 AstroLab Software
# Author: Abhishek Chauhan, Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import io
import json
import fastavro
import confluent_kafka

from fink_client.avroUtils import write_alert
from fink_client.avroUtils import _get_alert_schema
from fink_client.avroUtils import _decode_avro_alert

class AlertError(Exception):
    pass


class AlertConsumer:
    """
    High level Kafka consumer to receive alerts from Fink broker
    """

    def __init__(self, topics: list, config: dict, schema_path=None):
        """Creates an instance of `AlertConsumer`

        Parameters
        ----------
        topics : list of str
            list of topics to subscribe
        config: dict
            Dictionary of configurations. Allowed keys are:
            username: str
                username for API access
            password: str
                password for API access
            group_id: str
                group.id for Kafka consumer
            bootstrap.servers: str, optional
                Kafka servers to connect to
        """
        self._topics = topics
        self._kafka_config = _get_kafka_config(config)
        self.schema_path = schema_path
        self._consumer = confluent_kafka.Consumer(self._kafka_config)
        self._consumer.subscribe(self._topics)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._consumer.close()

    def poll(self, timeout: float = -1) -> (str, dict):
        """ Consume one message from Fink server

        Parameters
        ----------
        timeout: float, optional
            maximum time to block waiting for a message
            if not set default is None i.e. wait indefinitely

        Returns
        ----------
        (topic, alert, key): tuple(str, dict, str)
            returns (None, None, None) on timeout
        """
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None, None, None

        # msg.error() returns None or KafkaError
        if msg.error():
            error_message = """
            Error: {} topic: {}[{}] at offset: {} with key: {}
            """.format(
                msg.error(), msg.topic(),
                msg.partition(), msg.offset(),
                str(msg.key())
            )
            raise AlertError(error_message)

        topic = msg.topic()

        # decode the key if it is bytes
        key = msg.key()

        if type(key) == bytes:
            key = key.decode('utf8')
        if key is None:
            # compatibility with previous scheme
            key = ""

        try:
            _parsed_schema = fastavro.schema.parse_schema(json.loads(key))
            alert = self._decode_msg(_parsed_schema, msg)
        except json.JSONDecodeError:
            # Old way
            if self.schema_path is not None:
                _parsed_schema = _get_alert_schema(schema_path=self.schema_path)
                alert = self._decode_msg(_parsed_schema, msg)
            elif key is not None:
                try:
                    _parsed_schema = _get_alert_schema(key=key)
                    alert = self._decode_msg(_parsed_schema, msg)
                except IndexError:
                    _parsed_schema = _get_alert_schema(key=key + '_replayed')
                    alert = self._decode_msg(_parsed_schema, msg)
            else:
                msg = """
                The message cannot be decoded as there is no key (None). Alternatively
                specify manually the schema path when instantiating ``AlertConsumer`` (or from fink_consumer).
                """
                raise NotImplementedError(msg)

        return topic, alert, key

    def _decode_msg(self, parsed_schema, msg) -> dict:
        """ decode message using parsed schema

        Parameters
        ----------
        parsed_schema: dict
            Dictionary of json format schema for decoding avro alerts from Fink.
            Output of _get_alert_schema
        msg: bytes
            Message received

        Returns
        ----------
        alert: dict
            Decoded message
        """
        self._parsed_schema = parsed_schema
        avro_alert = io.BytesIO(msg.value())
        return _decode_avro_alert(avro_alert, self._parsed_schema)

    def consume(self, num_alerts: int = 1, timeout: float = -1) -> list:
        """ Consume and return list of messages

        Parameters
        ----------
        num_messages: int
            maximum number of messages to return
        timeout: float
            maximum time to block waiting for messages
            if not set default is None i.e. wait indefinitely

        Returns
        ----------
        list: [tuple(str, dict, str)]
            list of topic, alert, key
            returns an empty list on timeout
        """
        alerts = []
        msg_list = self._consumer.consume(num_alerts, timeout)

        for msg in msg_list:
            if msg is None:
                alerts.append((None, None, None))
                continue

            topic = msg.topic()

            # decode the key if it is bytes
            key = msg.key()

            if type(key) == bytes:
                key = key.decode('utf8')
            if key is None:
                # compatibility with previous scheme
                key = ""

            try:
                _parsed_schema = fastavro.schema.parse_schema(json.loads(key))
                alert = self._decode_msg(_parsed_schema, msg)
            except json.JSONDecodeError:
                # Old way
                if self.schema_path is not None:
                    _parsed_schema = _get_alert_schema(schema_path=self.schema_path)
                    alert = self._decode_msg(_parsed_schema, msg)
                elif key is not None:
                    try:
                        _parsed_schema = _get_alert_schema(key=key)
                        alert = self._decode_msg(_parsed_schema, msg)
                    except IndexError:
                        _parsed_schema = _get_alert_schema(key=key + '_replayed')
                        alert = self._decode_msg(_parsed_schema, msg)
                else:
                    msg = """
                    The message cannot be decoded as there is no key (None). Alternatively
                    specify manually the schema path when instantiating ``AlertConsumer`` (or from fink_consumer).
                    """
                    raise NotImplementedError(msg)

            alerts.append((topic, alert, key))

        return alerts

    def poll_and_write(
            self, outdir: str, timeout: float = -1,
            overwrite: bool = False) -> (str, dict):
        """ Consume one message from Fink server, save alert on disk and
        return (topic, alert, key)

        Parameters
        ----------
        outdir: str
            Folder to store the alert. It must exists.
        timeout: float, optional
            maximum time to block waiting for messages
            if not set default is None i.e. wait indefinitely
        overwrite: bool, optional
            If True, allow an existing alert to be overwritten.
            Default is False.

        Returns
        ----------
        (topic, alert): tuple(str, dict)
            returns (None, None) on timeout

        """
        topic, alert, key = self.poll(timeout)

        if topic is not None:
            write_alert(alert, self._parsed_schema, outdir, overwrite=overwrite)

        return topic, alert, key

    def available_topics(self) -> dict:
        """ Return available broker topics

        Note, this routine only display topics, but users need
        to be authorized to poll data.

        Returns
        ---------
        topics: dict
            Keys are topic names, values are metadata

        """
        return self._consumer.list_topics().topics

    def available_brokers(self) -> dict:
        """ Return available brokers

        Returns
        ---------
        brokers: dict
            Keys are broker ID, values are metadata with IP:PORT

        """
        return self._consumer.list_topics().brokers

    def close(self):
        """Close connection to Fink broker"""
        self._consumer.close()


def _get_kafka_config(config: dict) -> dict:
    """Returns configurations for a consumer instance

    Parameters
    ----------
    config: dict
        Dictionary of configurations

    Returns
    ----------
    kafka_config: dict
        Dictionary with configurations for creating an instance of
        a secured Kafka consumer
    """
    kafka_config = {}
    default_config = {
        "auto.offset.reset": "earliest"
    }

    if 'username' in config and 'password' in config:
        kafka_config["security.protocol"] = "sasl_plaintext"
        kafka_config["sasl.mechanism"] = "SCRAM-SHA-512"
        kafka_config["sasl.username"] = config["username"]
        kafka_config["sasl.password"] = config["password"]

    kafka_config["group.id"] = config["group_id"]

    kafka_config.update(default_config)

    # use servers if given
    if 'bootstrap.servers' in config:
        kafka_config["bootstrap.servers"] = config["bootstrap.servers"]
    else:
        # use default fink_servers
        fink_servers = [
            "localhost:9093",
            "localhost:9094",
            "localhost:9095"
        ]
        kafka_config["bootstrap.servers"] = "{}".format(",".join(fink_servers))

    return kafka_config
