#!/usr/bin/env python
# Copyright 2019-2026 AstroLab Software
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
"""Kafka consumer to listen and archive Fink streams from the Livestream service"""

import sys
import os


from fink_client.consumer import AlertConsumer
from fink_client.handlers import display_alerts_as_table
from fink_client.handlers import store_alert
from fink_client.handlers import send_to_telegram
from fink_client.configuration import load_credentials
from fink_client.configuration import mm_topic_names

from fink_client.consumer import print_offsets


def stream_(
    survey,
    limit,
    start_at,
    outdir,
    ext_schema,
    display_statistics,
    display,
    save,
    telegram,
    slack,
    dump_schema,
):
    """Wrapper around Kafka consumer to listen to Fink streams"""
    # load user configuration
    conf = load_credentials(survey=survey)

    kafka_config = {
        "bootstrap.servers": conf["servers"],
        "group.id": conf["groupid"],
    }

    if conf.get("password", None) is not None:
        kafka_config["password"] = conf["password"]

    if display_statistics:
        print()
        for topic in conf["topics"]:
            _, _ = print_offsets(kafka_config, topic, conf["maxtimeout"], verbose=True)
            print()
        sys.exit(0)

    # Instantiate a consumer
    if ext_schema is None:
        schema = None
    else:
        # FIXME: should check the file exist
        schema = ext_schema

    if start_at != "":
        if start_at == "earliest":

            def assign_offset(consumer, partitions):
                print("Resetting offsets to BEGINNING")
                for p in partitions:
                    low, _ = consumer.get_watermark_offsets(p)
                    p.offset = low
                    print("assign", p)
                consumer.assign(partitions)

        elif start_at == "latest":

            def assign_offset(consumer, partitions):
                print("Resetting offsets to END")
                for p in partitions:
                    _, high = consumer.get_watermark_offsets(p)
                    p.offset = high
                    print("assign", p)
                consumer.assign(partitions)

        else:
            raise AttributeError(
                "{} not recognized. -start_at should be `earliest` or `latest`.".format(
                    start_at
                )
            )
    else:
        assign_offset = None

    consumer = AlertConsumer(
        topics=list(conf["topics"].keys()),
        config=kafka_config,
        survey=conf["survey"],
        schema_path=schema,
        dump_schema=dump_schema,
        on_assign=assign_offset,
    )

    # Time to wait before polling again if no alerts
    maxtimeout = conf["maxtimeout"]

    if not os.path.isdir(outdir):
        os.makedirs(outdir, exist_ok=True)

    # infinite loop
    maxpoll = limit if limit else 1e10
    try:
        poll_number = 0
        while poll_number < maxpoll:
            topic, alert, _ = consumer.poll(timeout=maxtimeout)

            if alert is None:
                if display:
                    print("No alerts the last {} seconds".format(maxtimeout))
            else:
                poll_number += 1
                is_mma = topic in mm_topic_names()

                if save:
                    store_alert(
                        alert,
                        consumer._parsed_schema,
                        survey=survey,
                        is_mma=is_mma,
                        outdir=outdir,
                        overwrite=True,
                    )

                if telegram:
                    ext = conf["topics"].get(topic, None)
                    if "telegram" not in ext:
                        print(f"""
                        You should register telegram information for the topic {topic}:
                        finkctl topic subscribe -survey lsst -name {topic} -telegram_token $TOKEN -telegram_channel $CHANNEL
                        """)
                    token = ext["telegram"].get("token", None)
                    channel = ext["telegram"].get("channel", None)
                    if token is None or channel is None:
                        print(
                            "Token or channel missing for Telegram for the topic {}. Please check credentials ".format(
                                topic
                            )
                        )
                        break
                    send_to_telegram(alert, survey, token, channel)

                if slack:
                    pass

                if display:
                    display_alerts_as_table(conf["survey"], topic, alert, is_mma)

    except KeyboardInterrupt:
        sys.stderr.write("%% Aborted by user\n")
    finally:
        consumer.close()
