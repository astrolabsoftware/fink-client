#!/usr/bin/env python
# Copyright 2019-2023 AstroLab Software
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
""" Kafka consumer to listen and archive Fink streams from the Livestream service """
import sys

import argparse
import time

from tabulate import tabulate

from fink_client.consumer import AlertConsumer
from fink_client.configuration import load_credentials

def main():
    """ """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--display', action='store_true',
        help="If specified, print on screen information about incoming alert.")
    parser.add_argument(
        '-limit', type=int, default=None,
        help="If specified, download only `limit` alerts. Default is None.")
    parser.add_argument(
        '--available_topics', action='store_true',
        help="If specified, print on screen information about available topics.")
    parser.add_argument(
        '--save', action='store_true',
        help="If specified, save alert data on disk (Avro). See also -outdir.")
    parser.add_argument(
        '-outdir', type=str, default='.',
        help="Folder to store incoming alerts if --save is set. It must exist.")
    parser.add_argument(
        '-schema', type=str, default=None,
        help="Avro schema to decode the incoming alerts. Default is None (version taken from each alert)")
    args = parser.parse_args(None)

    # load user configuration
    conf = load_credentials()

    myconfig = {
        "username": conf['username'],
        'bootstrap.servers': conf['servers'],
        'group_id': conf['group_id']}

    if conf['password'] is not None:
        myconfig['password'] = conf['password']

    # Instantiate a consumer
    if args.schema is None:
        schema = None
    else:
        schema = args.schema
    consumer = AlertConsumer(conf['mytopics'], myconfig, schema_path=schema)

    if args.available_topics:
        print(consumer.available_topics().keys())
        sys.exit(0)

    # Time to wait before polling again if no alerts
    maxtimeout = conf['maxtimeout']

    # infinite loop
    maxpoll = args.limit if args.limit else 1e10
    try:
        poll_number = 0
        while poll_number < maxpoll:
            if args.save:
                # Save alerts on disk
                topic, alert, key = consumer.poll_and_write(
                    outdir=args.outdir,
                    timeout=maxtimeout,
                    overwrite=True
                )
            else:
                # TODO: this is useless to get it and done nothing
                # why not thinking about handler like Comet?
                topic, alert, key = consumer.poll(timeout=maxtimeout)

            if topic is not None:
                poll_number += 1

            if args.display and topic is not None:
                utc = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
                table = [
                    [
                        alert['timestamp'], utc, topic, alert['objectId'],
                        alert['cdsxmatch'],
                        alert['candidate']['magpsf']
                    ],
                ]
                headers = [
                    'Emitted at (UTC)', 'Received at (UTC)',
                    'Topic', 'objectId', 'Simbad', 'Magnitude'
                ]
                print(tabulate(table, headers, tablefmt="pretty"))
            elif args.display:
                print('No alerts the last {} seconds'.format(maxtimeout))
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
