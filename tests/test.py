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
import unittest
import os
import io
import confluent_kafka
import fastavro
from fink_client.consumer import AlertConsumer
from fink_client.avro_utils import AlertReader
from fink_client.avro_utils import encode_into_avro
from fink_client.avro_utils import get_legal_topic_name

from fink_client.configuration import load_credentials


class TestIntegration(unittest.TestCase):

    def setUp(self):

        data_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), 'data'))
        schema_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__),
            '../schemas/tests/distribution_schema_0p2.avsc'))

        r = AlertReader(data_path)
        alerts = r.to_list()

        conf = load_credentials(tmp=True)

        kafka_servers = conf['servers']
        p = confluent_kafka.Producer({
            'bootstrap.servers': kafka_servers})

        for alert in alerts:
            avro_data = encode_into_avro(alert, schema_path)
            topic = get_legal_topic_name(alert['cdsxmatch'])
            p.produce(topic, avro_data)
        p.flush()

        # instantiate an AlertConsumer
        mytopics = conf["mytopics"]

        myconfig = {
            'bootstrap.servers': kafka_servers,
            'group.id': conf['group_id']}

        self.consumer = AlertConsumer(mytopics, myconfig, schema_path=schema_path)

    def test_poll(self):
        topic, alert, key = self.consumer.poll()
        self.assertIsNotNone(alert)
        self.assertTrue(fastavro.validate(alert, self.consumer._parsed_schema))

    def test_consume(self):
        num_messages = 1
        alerts = self.consumer.consume(num_messages)
        self.assertEqual(len(alerts), num_messages)

    def test_topics(self):
        topics = self.consumer.available_topics()
        self.assertTrue('rrlyr' in topics.keys())

    def test_broker_name(self):
        brokers = self.consumer.available_brokers()
        self.assertTrue(0 in brokers.keys())

    def tearDown(self):
        self.consumer.close()


class TestComponents(unittest.TestCase):

    # def test_get_alert_schema(self):
    #     # download and check if a valid schema is downloaded
    #     from fink_client.consumer import _get_alert_schema
    #     schema = _get_alert_schema()
    #     self.assertIsInstance(schema, dict)

    def test_get_kafka_config(self):
        from fink_client.consumer import _get_kafka_config
        myconfig = {
            "username": "Alice",
            "password": "Alice-secret",
            "group.id": "test_group"
        }
        kafka_config = _get_kafka_config(myconfig)

        valid_config = (
            "security.protocol" in kafka_config and
            "sasl.mechanism" in kafka_config and
            "group.id" in kafka_config and
            "bootstrap.servers" in kafka_config
        )

        self.assertTrue(valid_config)

    def test_decode_avro_alert(self):
        from fink_client.consumer import _decode_avro_alert
        schema = {
            'name': 'test',
            'type': 'record',
            'fields': [
                {'name': 'name', 'type': 'string'},
                {'name': 'fav_num', 'type': 'int'}
            ]
        }
        record = {u'name': u'Alice', u'fav_num': 63}

        b = io.BytesIO()
        fastavro.schemaless_writer(b, schema, record)
        read_record = _decode_avro_alert(b, schema)

        self.assertDictEqual(record, read_record)


if __name__ == "__main__":
    unittest.main()
