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
import json
import glob
import fastavro
from typing import Iterable

def read_avro_alerts(data_path: str) -> Iterable[dict]:
    """ Read avro alert files and return an iterable
    with dicts of alert data

    Parameters
    ----------
    data_path: str
        a directory path where to look for avro alert files

    Returns
    ----------
    record: Iterable
        a generator that yields records (dict) after reading avro files
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


def encode_into_avro(alert: dict, schema_file: str) -> str:
    """Encode a dict record into avro bytes

    Parameters
    ----------
    alert: dict
        A Dictionary of alert data
    schema_file: str
        Path of avro schema file

    Returns
    ----------
    value: str
        a bytes string with avro encoded alert data
    """
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
