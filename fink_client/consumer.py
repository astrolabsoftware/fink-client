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
import io
import os
import time
import json
import confluent_kafka
import fastavro

class AlertConsumer:
    """
    High level Kafka consumer to receive alerts from Fink broker
    """
    
    def __init__(self, topics, config):
        """Creates an instance of `AlertConsumer`
        
        Parameters
        ----------
        topics : list of str
            list of topics to subscribe
        config: dict
            Dictionary of configurations
            
            api_user: str
                username for API access
            api_secret: str
                password for API access
            group_id: str
                group.id for Kafka consumer
        """
        _FINK_SERVERS = [
            "localhost:9093",
            "localhost:9094",
            "localhost:9095"
        ]
        self._topics = topics
        self._kafka_config = _get_kafka_config(_FINK_SERVERS, config)
        
        self._consumer = confluent_kafka.Consumer(self._kafka_config)
        self._consumer.subscribe(self._topics)
        
        self._parsed_schema = _get_alert_schema()
        
    def poll(self, timeout=None):
        """Consume messages from Fink server
        
        Parameters
        ----------
        timeout: float
            maximum time to block waiting for a message
            if not set default is None i.e. wait infinitely
        
        Returns
        ----------
        (topic, alert): tuple(str, dict)
            returns (None, None) on timeout
        """
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None, None
        
        topic = msg.topic()
        avro_alert = io.BytesIO(msg.value())
        alert = _decode_avro_alert(avro_alert, self._parsed_schema)
        
        return topic, alert
        
        
def _get_kafka_config(servers, config):
    
    kafka_config = {}
    default_config = {
        "bootstrap.servers": "{}".format(",".join(servers)),
        "auto.offset.reset": "earliest",
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-512"
    }
    
    invalid_config = False
    if 'username' not in config:
        print("please set username in config")
        invalid_config = True
        
    if 'password' not in config:
        print("please set password in config")
        invalid_config = True
        
    if 'group_id' not in config:
        print("please set group_id in config")
        invalid_config = True
    
    if invalid_config:
        return None
    
    kafka_config.update(default_config)
    
    kafka_config["sasl.username"] = config["username"]
    kafka_config["sasl.password"] = config["password"]
    kafka_config["group.id"] = config["group_id"]

    return kafka_config


def _get_alert_schema():
    schema_path = os.path.abspath(os.path.join(
        os.pardir, 'schema/fink_alert_schema.avsc'))
    
    with open(schema_path) as f:
        schema = json.load(f)
    
    return fastavro.parse_schema(schema)


def _decode_avro_alert(avro_alert, schema):
    avro_alert.seek(0)
    return fastavro.schemaless_reader(avro_alert, schema)
