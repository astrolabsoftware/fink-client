#!/usr/bin/env python
# Copyright 2019-2024 AstroLab Software
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
import sys
import json
import time
import fastavro
import confluent_kafka
from datetime import datetime, timezone

from fink_client.avro_utils import write_alert
from fink_client.avro_utils import _get_alert_schema
from fink_client.avro_utils import _decode_avro_alert
from fink_client.configuration import mm_topic_names


class AlertError(Exception):
    """Base class for exception"""

    pass


class AlertConsumer:
    """High level Kafka consumer to receive alerts from Fink broker"""

    def __init__(
        self,
        topics: list,
        config: dict,
        schema_path=None,
        dump_schema=False,
        on_assign=None,
    ):
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
            schema_path: str, optional
                If specified, path to an alert schema (avsc).
                Default is None.
            dump_schema: bool, optional
                If True, save incoming alert schema on disk.
                Useful for schema inspection when getting `IndexError`.
                Default is False.
            on_assign: callable, optional
                Callback to update the current assignment
                and specify start offsets. Default is None.

        """
        self._topics = topics
        self._kafka_config = _get_kafka_config(config)
        self.schema_path = schema_path
        self._consumer = confluent_kafka.Consumer(self._kafka_config)

        if on_assign is not None:
            self._consumer.subscribe(self._topics, on_assign=on_assign)
        else:
            self._consumer.subscribe(self._topics)
        self.dump_schema = dump_schema

    def __enter__(self):
        """Enter"""
        return self

    def __exit__(self, type, value, traceback):
        """Exit"""
        self._consumer.close()

    def process_message(self, msg):
        """Process message from Kafka

        Parameters
        ----------
        msg: confluent_kafka.Message
            Object containing message information

        Returns
        -------
        list: [tuple(str, dict, str)]
            list of topic, alert, key
            returns an empty list on timeout
        """
        # msg.error() returns None or KafkaError
        if msg.error():
            error_message = """
            Error: {} topic: {}[{}] at offset: {} with key: {}
            """.format(
                msg.error(), msg.topic(), msg.partition(), msg.offset(), str(msg.key())
            )
            raise AlertError(error_message)

        topic = msg.topic()

        # decode the key if it is bytes
        key = msg.key()

        if isinstance(key, bytes):
            key = key.decode("utf8")
        if key is None:
            # compatibility with previous scheme
            key = ""

        try:
            _parsed_schema = fastavro.schema.parse_schema(json.loads(key))
            if self.dump_schema:
                today = datetime.now(timezone.utc).isoformat()
                filename = "schema_{}.json".format(today)
                with open(filename, "w") as json_file:
                    json.dump(_parsed_schema, json_file, sort_keys=True, indent=4)
                print("Schema saved as {}".format(filename))
            alert = self._decode_msg(_parsed_schema, msg)
        except json.JSONDecodeError as exc:
            # Old way
            if self.schema_path is not None:
                _parsed_schema = _get_alert_schema(schema_path=self.schema_path)
                alert = self._decode_msg(_parsed_schema, msg)
            elif key is not None:
                try:
                    _parsed_schema = _get_alert_schema(key=key)
                    alert = self._decode_msg(_parsed_schema, msg)
                except IndexError:
                    _parsed_schema = _get_alert_schema(key=key + "_replayed")
                    alert = self._decode_msg(_parsed_schema, msg)
            else:
                msg = """
                The message cannot be decoded as there is no key (None). Alternatively
                specify manually the schema path when instantiating ``AlertConsumer`` (or from fink_consumer).
                """
                raise NotImplementedError(msg) from exc

        return topic, alert, key

    def poll(self, timeout: float = -1) -> (str, dict):
        """Consume one message from Fink server

        Parameters
        ----------
        timeout: float, optional
            maximum time to block waiting for a message
            if not set default is None i.e. wait indefinitely

        Returns
        -------
        (topic, alert, key): tuple(str, dict, str)
            returns (None, None, None) on timeout
        """
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None, None, None

        return self.process_message(msg)

    def _decode_msg(self, parsed_schema, msg) -> dict:
        """Decode message using parsed schema

        Parameters
        ----------
        parsed_schema: dict
            Dictionary of json format schema for decoding avro alerts from Fink.
            Output of _get_alert_schema
        msg: bytes
            Message received

        Returns
        -------
        alert: dict
            Decoded message
        """
        self._parsed_schema = parsed_schema
        avro_alert = io.BytesIO(msg.value())
        return _decode_avro_alert(avro_alert, self._parsed_schema)

    def consume(self, num_alerts: int = 1, timeout: float = -1) -> list:
        """Consume and return list of messages

        Parameters
        ----------
        num_messages: int
            maximum number of messages to return
        timeout: float
            maximum time to block waiting for messages
            if not set default is None i.e. wait indefinitely

        Returns
        -------
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

            topic, alert, key = self.process_message(msg)

            alerts.append((topic, alert, key))

        return alerts

    def poll_and_write(
        self, outdir: str, timeout: float = -1, overwrite: bool = False
    ) -> (str, dict):
        """Consume one message from Fink server

        Notes
        -----
        It also saves the alert on disk and return (topic, alert, key)

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
        -------
        (topic, alert): tuple(str, dict)
            returns (None, None) on timeout

        """
        topic, alert, key = self.poll(timeout)
        is_mma = topic in mm_topic_names()

        if is_mma:
            id1 = "objectId"
            id2 = "triggerId"
        else:
            id1 = "objectId"
            id2 = "candid"

        if topic is not None:
            write_alert(
                alert,
                self._parsed_schema,
                outdir,
                overwrite=overwrite,
                id1=id1,
                id2=id2,
            )

        return topic, alert, key

    def available_topics(self) -> dict:
        """Return available broker topics

        Note, this routine only display topics, but users need
        to be authorized to poll data.

        Returns
        -------
        topics: dict
            Keys are topic names, values are metadata

        """
        return self._consumer.list_topics().topics

    def available_brokers(self) -> dict:
        """Return available brokers

        Returns
        -------
        brokers: dict
            Keys are broker ID, values are metadata with IP:PORT

        """
        return self._consumer.list_topics().brokers

    def close(self):
        """Close connection to Fink broker"""
        self._consumer.close()


def return_offsets(
    consumer, topic, waitfor=1, timeout=10, hide_empty_partition=True, verbose=False
):
    """Poll servers to get the total committed offsets, and remaining lag

    Parameters
    ----------
    consumer: confluent_kafka.Consumer
        Kafka consumer
    topic: str
        Topic name
    waitfor: int, optional
        Time in second to wait before polling. Default is 1 second.
    timeout: int, optional
        Timeout in second when polling the servers. Default is 10.
    hide_empty_partition: bool, optional
        If True, display only non-empty partitions.
        Default is True
    verbose: bool, optional
        If True, prints useful table. Default is False.

    Returns
    -------
    total_offsets: int
        Total number of messages committed across all partitions
    total_lag: int
        Remaining messages in the topic across all partitions.
    """
    time.sleep(waitfor)
    # Get the topic's partitions
    metadata = consumer.list_topics(topic, timeout=timeout)
    if metadata.topics[topic].error is not None:
        raise confluent_kafka.KafkaException(metadata.topics[topic].error)

    # Construct TopicPartition list of partitions to query
    partitions = [
        confluent_kafka.TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions
    ]

    # Query committed offsets for this group and the given partitions
    try:
        committed = consumer.committed(partitions, timeout=timeout)
    except confluent_kafka.KafkaException as exception:
        kafka_error = exception.args[0]
        if kafka_error.code() == confluent_kafka.KafkaError._TIMED_OUT:
            return -1, -1
        else:
            return 0, 0

    total_offsets = 0
    total_lag = 0
    if verbose:
        print("%-50s  %9s  %9s" % ("Topic [Partition]", "Committed", "Lag"))
        print("=" * 72)
    for partition in committed:
        # Get the partitions low and high watermark offsets.
        (lo, hi) = consumer.get_watermark_offsets(
            partition, timeout=timeout, cached=False
        )

        if partition.offset == confluent_kafka.OFFSET_INVALID:
            offset = "-"
        else:
            offset = "%d" % (partition.offset)

        if hi < 0:
            lag = 0  # Unlikely
        elif partition.offset < 0:
            # No committed offset, show total message count as lag.
            # The actual message count may be lower due to compaction
            # and record deletions.
            lag = hi - lo
            partition.offset = 0
        else:
            lag = hi - partition.offset
        #
        total_offsets = total_offsets + partition.offset
        total_lag = total_lag + int(lag)

        if verbose:
            if (hide_empty_partition and (offset != "-" or int(lag) > 0)) or (
                not hide_empty_partition
            ):
                print(
                    "%-50s  %9s  %9s"
                    % (
                        "{} [{}]".format(partition.topic, partition.partition),
                        offset,
                        lag,
                    )
                )
    if verbose:
        print("-" * 72)
        print(
            "%-50s  %9s  %9s" % ("Total for {}".format(topic), total_offsets, total_lag)
        )
        print("-" * 72)

    return total_offsets, total_lag


def return_last_offsets(kafka_config, topic):
    """Return the last offsets

    Parameters
    ----------
    kafka_config: dict
        Kafka consumer config
    topic: str
        Topic name

    Returns
    -------
    offsets: list
        Last offsets of each partition
    """
    consumer = confluent_kafka.Consumer(kafka_config)
    topics = ["{}".format(topic)]
    consumer.subscribe(topics)

    metadata = consumer.list_topics(topic)
    if metadata.topics[topic].error is not None:
        raise confluent_kafka.KafkaException(metadata.topics[topic].error)

    # List of partitions
    partitions = [
        confluent_kafka.TopicPartition(topic, p)
        for p in metadata.topics[topic].partitions
    ]
    committed = consumer.committed(partitions)
    offsets = []
    for partition in committed:
        if partition.offset != confluent_kafka.OFFSET_INVALID:
            offsets.append(partition.offset)
        else:
            offsets.append(0)

    consumer.close()
    return offsets


def print_offsets(
    kafka_config, topic, maxtimeout=10, hide_empty_partition=True, verbose=True
):
    """Wrapper around `consumer.return_offsets`

    If the server is rebalancing the offsets, it will exit the program.

    Parameters
    ----------
    kafka_config: dic
        Dictionary with consumer parameters
    topic: str
        Topic name
    maxtimeout: int, optional
        Timeout in second, when polling the servers
    hide_empty_partition: bool, optional
        If True, display only non-empty partitions.
        Default is True
    verbose: bool, optional
        If True, prints useful table. Default is True.

    Returns
    -------
    total_offsets: int
        Total number of messages committed across all partitions
    total_lag: int
        Remaining messages in the topic across all partitions.
    """
    consumer = confluent_kafka.Consumer(kafka_config)

    topics = ["{}".format(topic)]
    consumer.subscribe(topics)
    total_offset, total_lag = return_offsets(
        consumer,
        topic,
        timeout=maxtimeout,
        waitfor=0,
        verbose=verbose,
        hide_empty_partition=hide_empty_partition,
    )
    if (total_offset, total_lag) == (-1, -1):
        print(
            "Warning: Consumer group '{}' is rebalancing. Please wait.".format(
                kafka_config["group.id"]
            )
        )
        sys.exit()
    consumer.close()

    return total_lag, total_offset


def _get_kafka_config(config: dict) -> dict:
    """Returns configurations for a consumer instance

    Parameters
    ----------
    config: dict
        Dictionary of configurations

    Returns
    -------
    kafka_config: dict
        Dictionary with configurations for creating an instance of
        a secured Kafka consumer
    """
    kafka_config = {}
    default_config = {"auto.offset.reset": "earliest"}

    if "username" in config and "password" in config:
        kafka_config["security.protocol"] = "sasl_plaintext"
        kafka_config["sasl.mechanism"] = "SCRAM-SHA-512"
        kafka_config["sasl.username"] = config["username"]
        kafka_config["sasl.password"] = config["password"]

    kafka_config["group.id"] = config["group.id"]

    kafka_config.update(default_config)

    # use servers if given
    if "bootstrap.servers" in config:
        kafka_config["bootstrap.servers"] = config["bootstrap.servers"]
    else:
        # use default fink_servers
        fink_servers = ["localhost:9093", "localhost:9094", "localhost:9095"]
        kafka_config["bootstrap.servers"] = "{}".format(",".join(fink_servers))

    return kafka_config


def return_npartitions(topic, kafka_config):
    """Get the number of partitions

    Parameters
    ----------
    kafka_config: dic
        Dictionary with consumer parameters
    topic: str
        Topic name

    Returns
    -------
    nbpartitions: int
        Number of partitions in the topic

    """
    consumer = confluent_kafka.Consumer(kafka_config)

    # Details to get
    nbpartitions = 0
    try:
        # Topic metadata
        metadata = consumer.list_topics(topic=topic)

        if metadata.topics and topic in metadata.topics:
            partitions = metadata.topics[topic].partitions
            nbpartitions = len(partitions)
        else:
            print("The topic {} does not exist".format(topic))

    except confluent_kafka.KafkaException as e:
        print(f"Error while getting the number of partitions: {e}")

    consumer.close()

    return nbpartitions


def return_partition_offset(consumer, topic, partition):
    """Return the offset and the remaining lag of a partition

    consumer: confluent_kafka.Consumer
        Kafka consumer
    topic: str
        Topic name
    partition: int
        The partition number

    Returns
    -------
    offset : int
        Total number of offsets in the topic
    """
    topicPartition = confluent_kafka.TopicPartition(topic, partition)
    low_offset, high_offset = consumer.get_watermark_offsets(topicPartition)
    partition_size = high_offset - low_offset

    return partition_size


def get_schema_from_stream(kafka_config, topic, maxtimeout):
    """Poll the schema data from the schema topic

    Parameters
    ----------
    kafka_config: dic
        Dictionary with consumer parameters
    topic: str
        Topic name
    timeout: int, optional
        Timeout in second, when polling the servers

    Returns
    -------
    schema: None or dic
        Schema data. None if the poll was not successful.
        Reasons to get None:
            1. timeout has been reached (increase timeout)
            2. topic is empty (produce new data)
            3. topic does not exist (create the topic)
    """
    # Instantiate a consumer
    consumer_schema = confluent_kafka.Consumer(kafka_config)

    # Subscribe to schema topic
    topics = ["{}_schema".format(topic)]
    consumer_schema.subscribe(topics)

    # Poll
    msg = consumer_schema.poll(maxtimeout)
    if msg is not None:
        schema = fastavro.schema.parse_schema(json.loads(msg.key()))
    else:
        schema = None

    consumer_schema.close()

    return schema
