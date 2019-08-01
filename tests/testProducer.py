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
import os
import io
import confluent_kafka
import json
import glob
import time
import fastavro
from pprint import pprint as pp
from typing import Iterable

def read_avro_alerts(data_path: str) -> Iterable[dict]:
    """ read avro alert files and return an interable 
    with dicts of alert data"""
    avro_files = glob.glob(data_path + '/*.avro')
    
    for avro_file in avro_files:
        # check for valid avro file
        if not fastavro.is_avro(avro_file):
            continue
        
        with open(avro_file, 'rb') as f:
            reader = fastavro.reader(f)
            record = next(reader)
    
        yield record


def encode_into_avro(alert: dict):
    schema_file = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '../tests/test_schema.avsc'))
    
    with open(schema_file) as f:
        schema = json.load(f)
    
    parsed_schema = fastavro.parse_schema(schema)
    b = io.BytesIO()
    fastavro.schemaless_writer(b, parsed_schema, alert)
    
    return b.getvalue()


def get_legal_topic_name(topic: str) -> str:
    """Special characters are not allowed in the name 
    of a Kafka topic. Returns a legal topic name"""
    legal_topic = ''.join(a.lower() for a in topic if a.isalpha())
    return legal_topic
    
    
def main():
    
    data_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), '../data'))
    
    # for debug
    print("datapath is :", data_path)
    
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
        time.sleep(5)


if __name__ == "__main__":
    main()
