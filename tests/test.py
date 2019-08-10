#!/usr/bin/env python
# Copyright 2019 AstroLab Software
# Author: Abhishek Chauhan
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
import unittest
import os
import io
import confluent_kafka
import json
import glob
import time
import fastavro
from typing import Iterable
from fink_client.consumer import AlertConsumer
from coverage import Coverage

def read_avro_alerts(data_path: str) -> Iterable[dict]:
    """ Read avro alert files and return an interable
    with dicts of alert data

    Parameters
    ----------
    data_path: str
        a directory path where to look for avro alert files

    Returns
    ----------
    record: Iterable
        a generator that yields records(dict) after reading avro files
        in the given directory
    """
    avro_files = glob.glob(data_path + '/*.avro')

    for avro_file in avro_files:
        # check for valid avro file
        if not fastavro.is_avro(avro_file):
            continue

        with open(avro_file, 'rb') as f:
            reader = fastavro.reader(f)
            record = next(reader)

        yield record


def encode_into_avro(alert: dict) -> str:
    """Encode a dict record into avro bytes

    Parameters
    ----------
    alert: dict
        A Dictionary of alert data

    Returns
    ----------
    value: str
        a bytes string with avro encoded alert data
    """
    schema_file = os.path.abspath(os.path.join(
        os.path.dirname(__file__), 'test_schema.avsc'))

    with open(schema_file) as f:
        schema = json.load(f)

    parsed_schema = fastavro.parse_schema(schema)
    b = io.BytesIO()
    fastavro.schemaless_writer(b, parsed_schema, alert)

    return b.getvalue()


def get_legal_topic_name(topic: str) -> str:
    """Returns a legal Kafka topic name

    Special characters are not allowed in the name
    of a Kafka topic. This method returns a legal name
    after removing special characters and converting each
    letter to lowercase

    Parameters
    ----------
    topic: str
        topic name, essentially an alert parameter which is to be used
        to create a topic

    Returns
    ----------
    legal_topic: str
        A topic name that can be used as a Kafka topic
    """
    legal_topic = ''.join(a.lower() for a in topic if a.isalpha())
    return legal_topic


class TestIntegration(unittest.TestCase):

    def setUp(self):

        data_path = os.path.abspath(os.path.join(
                os.path.dirname(__file__), 'data'))

        alert_reader = read_avro_alerts(data_path)

        kafka_servers = 'localhost:9093, localhost:9094, localhost:9095'
        p = confluent_kafka.Producer({
                'bootstrap.servers': kafka_servers})


        for alert in alert_reader:
            avro_data = encode_into_avro(alert)
            topic = get_legal_topic_name(
                    alert['cross_match_alerts_per_batch'])
            p.produce(topic, avro_data)
        p.flush()

        # instantiate an AlertConsumer
        mytopics = ["rrlyr", "ebwuma", "unknown"]
        test_schema = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "test_schema.avsc"))
        myconfig = {
                'bootstrap.servers': kafka_servers,
                'group_id': 'test_group'}

        self.consumer = AlertConsumer(mytopics, myconfig, schema=test_schema)

    def test_poll(self):
        alert, topic = self.consumer.poll()
        self.assertIsNotNone(alert)

    def test_consume(self):
        num_messages = 3
        alerts = self.consumer.consume(num_messages)
        self.assertEqual(len(alerts), num_messages)

    def tearDown(self):
        self.consumer.close()


class TestComponents(unittest.TestCase):

    def test_get_alert_schema(self):
        # download and check if a valid schema is downloaded
        from fink_client.consumer import _get_alert_schema
        schema = _get_alert_schema()
        self.assertIsInstance(schema, dict)

    def test_get_kafka_config(self):
        from fink_client.consumer import _get_kafka_config
        myconfig = {
                "username": "Alice",
                "password": "Alice-secret",
                "group_id": "test_group"}
        kafka_config = _get_kafka_config(myconfig)

        valid_config = ("security.protocol" in kafka_config and
                "sasl.mechanism" in kafka_config and
                "group.id" in kafka_config and
                "bootstrap.servers" in kafka_config)

        self.assertTrue(valid_config)


if __name__ == "__main__":
    _data = os.path.abspath(os.path.join(
            os.path.dirname(__file__), "../.coverage"))
    _config = os.path.abspath(os.path.join(
            os.path.dirname(__file__), "../.coveragerc"))
    cov = Coverage(data_file=_data, config_file=_config, cover_pylib=False)
    cov.start()
    unittest.main(exit=False)
    cov.stop()
    cov.save()
    print("Total coverage:", cov.report())
