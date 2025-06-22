#!/usr/bin/env python
# Copyright 2023-2024 AstroLab Software
# Author: Julien Peloton, Saikou Oumar BAH
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
"""Kafka consumer to listen and archive Fink streams from the data transfer service"""

import sys
import os
import io
import argparse
import logging
import psutil
import json

from tqdm import trange

import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import confluent_kafka

import pandas as pd
import numpy as np
from astropy.time import Time

from multiprocessing import Process, Queue

from fink_client.configuration import load_credentials

from fink_client.consumer import (
    print_offsets,
    return_npartitions,
    return_partition_offset,
    return_last_offsets,
    get_schema_from_stream,
)
from fink_client.logger import get_fink_logger

_LOG = get_fink_logger("Fink", "INFO")


def poll(process_id, nconsumers, queue, schema, kafka_config, rng, args):
    """Poll data from Kafka servers

    Parameters
    ----------
    process_id: int
        ID of the process used for multiprocessing
    nconsumers: int
        Number of consumers (cores) in parallel
    queue: Multiprocessing.Queue
        Shared queue between processes where are stocked partitions
        and the last offset of the partition
    schema: dict
        Alert schema
    kafka_config: dict
        Configuration to instantiate a consumer
    args: dict
        Other arguments (topic, maxtimeout, total_offset, total_lag)
        required for the processing
    """
    # Instantiate a consumer
    consumer = confluent_kafka.Consumer(kafka_config)
    # Subscribe to schema topic
    # topics = ['{}'.format(args.topic)]

    # infinite loop
    maxpoll = int(args.limit / nconsumers) if args.limit is not None else 1e10
    disable = not args.verbose

    poll_number = 0
    while not queue.empty() and poll_number < maxpoll:
        # Getting a partition from the queue
        partition = queue.get()
        tp = confluent_kafka.TopicPartition(
            args.topic, partition["partition"], offset=partition["offset"]
        )
        consumer.assign([tp])
        # Getting the total number of alert in the partition
        offset = return_partition_offset(consumer, args.topic, partition["partition"])
        # Resuming from the last consumed alert
        initial = partition["offset"]

        max_end_check = 4

        if offset == initial:
            if partition["status"] < max_end_check:
                # After max_end_check time if no alerts added,
                # it is supposed finished
                queue.put({
                    "partition": partition["partition"],
                    "offset": partition["offset"],
                    "status": partition["status"] + 1,
                })
        else:
            poll_number = initial
            total = offset
            with trange(
                total,
                position=partition["partition"],
                initial=initial,
                colour="#F5622E",
                unit="alerts",
                disable=disable,
            ) as pbar:
                try:
                    while poll_number < maxpoll:
                        msgs = consumer.consume(args.batchsize, args.maxtimeout)
                        # Decode the message
                        if msgs is not None:
                            if len(msgs) == 0:
                                _LOG.warning(
                                    "[{}] No alerts the last {} seconds ({} polled)... Have to exit(1)\n".format(
                                        process_id, args.maxtimeout, poll_number
                                    )
                                )
                                # Alerts can be added in the partition later
                                # putting it again in the queue
                                # changing the offset to continue where we stopped
                                queue.put({
                                    "partition": partition["partition"],
                                    "offset": poll_number,
                                    "status": 0,
                                })
                                break

                            pdf = pd.DataFrame.from_records(
                                [
                                    fastavro.schemaless_reader(
                                        io.BytesIO(msg.value()), schema
                                    )
                                    for msg in msgs
                                ],
                            )
                            if pdf.empty:
                                break

                            # known mismatches between partitions
                            # see https://github.com/astrolabsoftware/fink-client/issues/165
                            if "cats_broad_max_prob" in pdf.columns:
                                pdf["cats_broad_max_prob"] = pdf[
                                    "cats_broad_max_prob"
                                ].astype("float")

                            if "cats_broad_class" in pdf.columns:
                                pdf["cats_broad_class"] = pdf[
                                    "cats_broad_class"
                                ].astype("float")

                            if "tracklet" in pdf.columns:
                                pdf["tracklet"] = pdf["tracklet"].astype("str")

                            if args.partitionby == "time":
                                if "jd" in pdf.columns:
                                    pdf[["year", "month", "day"]] = pdf[["jd"]].apply(
                                        lambda x: Time(x.iloc[0], format="jd")
                                        .strftime("%Y-%m-%d")
                                        .split("-"),
                                        axis=1,
                                        result_type="expand",
                                    )
                                elif "candidate" in pdf.columns:
                                    pdf[["year", "month", "day"]] = pdf[
                                        ["candidate"]
                                    ].apply(
                                        lambda x: Time(x.iloc[0]["jd"], format="jd")
                                        .strftime("%Y-%m-%d")
                                        .split("-"),
                                        axis=1,
                                        result_type="expand",
                                    )
                                partitioning = ["year", "month", "day"]
                            elif args.partitionby == "finkclass":
                                if "finkclass" not in pdf.columns:
                                    # partition by time
                                    # put a warning
                                    _LOG.warning(
                                        "finkclass not found. Applying time partitioning."
                                    )
                                    if "jd" in pdf.columns:
                                        pdf[["year", "month", "day"]] = pdf[
                                            ["jd"]
                                        ].apply(
                                            lambda x: Time(x.iloc[0], format="jd")
                                            .strftime("%Y-%m-%d")
                                            .split("-"),
                                            axis=1,
                                            result_type="expand",
                                        )
                                        partitioning = ["year", "month", "day"]
                                    else:
                                        # put an error
                                        pass
                                else:
                                    partitioning = ["finkclass"]
                            elif args.partitionby == "tnsclass":
                                partitioning = ["tnsclass"]
                            elif args.partitionby == "classId":
                                partitioning = ["classId"]

                            table = pa.Table.from_pandas(pdf)

                            if poll_number == initial:
                                table_schema = table.schema

                            part_num = rng.randint(0, 1e6)
                            try:
                                pq.write_to_dataset(
                                    table,
                                    args.outdir,
                                    schema=table_schema,
                                    basename_template="part-{}-{{i}}-{}.parquet".format(
                                        process_id, part_num
                                    ),
                                    partition_cols=partitioning,
                                    existing_data_behavior="overwrite_or_ignore",
                                )
                            except pa.lib.ArrowTypeError:
                                _LOG.warning(
                                    "Schema mismatch detected -- recreating the schema"
                                )
                                table_schema_ = table.schema
                                pq.write_to_dataset(
                                    table,
                                    args.outdir,
                                    schema=table_schema_,
                                    basename_template="part-{}-{{i}}-{}.parquet".format(
                                        process_id, part_num
                                    ),
                                    partition_cols=partitioning,
                                    existing_data_behavior="overwrite_or_ignore",
                                )

                            poll_number += len(msgs)
                            pbar.update(len(msgs))

                            if len(msgs) < args.batchsize:
                                queue.put({
                                    "partition": partition["partition"],
                                    "offset": poll_number,
                                    "status": 0,
                                })
                                break
                        else:
                            logging.info(
                                "[{}] No alerts the last {} seconds ({} polled)\n".format(
                                    process_id, args.maxtimeout, poll_number
                                )
                            )
                except KeyboardInterrupt:
                    sys.stderr.write("%% Aborted by user\n")
                    consumer.close()
    consumer.close()


def main():
    """ """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-topic",
        type=str,
        default=".",
        help="Topic name for the stream that contains the data.",
    )
    parser.add_argument(
        "-limit",
        type=int,
        default=None,
        help="If specified, download only `limit` alerts from the stream. Default is None, that is download all alerts.",
    )
    parser.add_argument(
        "-outdir",
        type=str,
        default=".",
        help="Folder to store incoming alerts. It will be created if it does not exist.",
    )
    parser.add_argument(
        "-partitionby",
        type=str,
        default="time",
        help="Partition data by `time` (year=YYYY/month=MM/day=DD), or `finkclass` (finkclass=CLASS), or `tnsclass` (tnsclass=CLASS). `classId` is also available for ELASTiCC data. Default is time.",
    )
    parser.add_argument(
        "-batchsize",
        type=int,
        default=1000,
        help="Maximum number of alert within the `maxtimeout` (see conf). Default is 1000 alerts.",
    )
    parser.add_argument(
        "-nconsumers",
        type=int,
        default=-1,
        help="Number of parallel consumer to use. Default (-1) is the number of logical CPUs in the system.",
    )
    parser.add_argument(
        "-maxtimeout",
        type=float,
        default=None,
        help="Overwrite the default timeout (in seconds) from user configuration. Default is None.",
    )
    parser.add_argument(
        "-number_partitions",
        type=int,
        default=10,
        help="Number of partitions for the topic in the distant Kafka cluster. Do not touch unless you know what your are doing. Default is 10 (Fink Kafka cluster)",
    )
    parser.add_argument(
        "--restart_from_beginning",
        action="store_true",
        help="If specified, restart downloading from the 1st alert in the stream. Default is False.",
    )
    parser.add_argument(
        "--dump_schema",
        action="store_true",
        help="If specified, save the schema on disk (json file)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="If specified, print on screen information about the consuming.",
    )
    args = parser.parse_args(None)

    if args.partitionby not in ["time", "finkclass", "tnsclass", "classId"]:
        _LOG.error(
            "{} is an unknown partitioning. `-partitionby` should be in ['time', 'finkclass', 'tnsclass', 'classId']".format(
                args.partitionby
            )
        )
        sys.exit()

    if not args.topic.startswith("ftransfer"):
        msg = """
{} is not a valid topic name.
Topic name must start with `ftransfer_`.
Check the webpage on which you submit the job,
and open the tab `Get your data` to retrieve the topic.
        """.format(args.topic)
        _LOG.error(msg)
        sys.exit()

    # load user configuration
    conf = load_credentials()

    # Time to wait before polling again if no alerts
    if args.maxtimeout is None:
        args.maxtimeout = conf["maxtimeout"]

    # Number of consumers to use
    if args.nconsumers == -1:
        nconsumers = psutil.cpu_count(logical=True)
    else:
        nconsumers = args.nconsumers

    kafka_config = {
        "bootstrap.servers": conf["servers"],
        "group.id": conf["group_id"],
        "auto.offset.reset": "earliest",
    }

    if args.restart_from_beginning:
        total_lag, total_offset = print_offsets(
            kafka_config,
            args.topic,
            args.maxtimeout,
            verbose=False,
            hide_empty_partition=False,
        )
        args.total_lag = total_offset
        args.total_offset = 0
        offsets = [0 for _ in range(args.number_partitions)]
    else:
        total_lag, total_offset = print_offsets(
            kafka_config, args.topic, args.maxtimeout, hide_empty_partition=False
        )
        args.total_lag = total_lag
        args.total_offset = total_offset
        offsets = return_last_offsets(kafka_config, args.topic)
        if total_lag == 0:
            _LOG.info("All alerts have been polled. Exiting.")
            sys.exit()

    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir, exist_ok=True)

    if (args.limit is not None) and (args.limit < args.batchsize):
        args.batchsize = args.limit

    schema = get_schema_from_stream(kafka_config, args.topic, args.maxtimeout)
    if schema is None:
        # TBD: raise error
        _LOG.info(
            "No schema found -- wait a few seconds and relaunch. If the error persists, maybe the queue is empty."
        )
    else:
        if args.dump_schema:
            filename = "schema_{}.json".format(args.topic)
            with open(filename, "w") as json_file:
                json.dump(schema, json_file, sort_keys=True, indent=4)
        nbpart = return_npartitions(args.topic, kafka_config)
        _LOG.info("Number of partitions for topic {}: {}".format(args.topic, nbpart))
        available = Queue()
        # Queue loading
        for key in range(nbpart):
            available.put({"partition": key, "offset": offsets[key], "status": 0})

        # Processes Creation
        random_state = 0
        rng = np.random.RandomState(random_state)
        procs = []
        for i in range(nconsumers):
            proc = Process(
                target=poll,
                args=(i, nconsumers, available, schema, kafka_config, rng, args),
            )
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()

        print_offsets(kafka_config, args.topic, args.maxtimeout)


if __name__ == "__main__":
    main()
