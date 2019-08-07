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

import time
from pprint import pprint as pp
from fink_client.consumer import AlertConsumer

def main():
    
    mytopics = ["rrlyr", "ebwuma", "unknown"]
    test_servers = "localhost:9093,localhost:9094,localhost:9095"
    test_schema = "test_schema.avsc"
    
    myconfig = {
            'bootstrap.servers': test_servers,
            'group_id': 'test_group'}
    
    consumer = AlertConsumer(mytopics, myconfig, schema=test_schema)
    
    # listent to alert stream for some time
    t_end = time.time() + 40
    
    while time.time() < t_end:
        topic, alert = consumer.poll(5)
        
        if alert is not None:
            print("Alert received on: ",topic)
            pp(alert)
            print("---" * 10)
    
    consumer.close()


if __name__ == "__main__":
    main()