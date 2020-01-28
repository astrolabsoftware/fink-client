# Copyright 2019 AstroLab Software
# Author: Julien Peloton
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
""" Configuration file for the fink-client API.
"""
# Fink broker credentials. Contact us to get them.
username = "finkConsumer"
password = "finkConsumer-secret"
group_id = "spark-kafka-client"

# Timeout when polling alerts (seconds)
maxtimeout = 2

# Allow to overwrite alerts and loop over a subset of inputs
testmode = True

# List of topic names you subscribed to
# See https://fink-broker.readthedocs.io/en/latest/distribution/introduction/
mytopics = ["rrlyr"]

# Servers from which data will be pulled
servers = "localhost:9092,localhost:9093"

# Incoming alert schema to decode the data
# If empty,the client will attempt to download the online latest version.
schema = "schemas/distribution_schema_0p2.avsc"

# Monitoring database. It includes metadata for the client to run.
# If it does not exist, it will be automatically created by the client.
db_path = 'db/alert-monitoring.db'
