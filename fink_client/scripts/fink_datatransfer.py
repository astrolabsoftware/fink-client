#!/usr/bin/env python
# Copyright 2023-2026 AstroLab Software
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
import psutil
import json
import time

from tqdm import trange, tqdm

import pyarrow.parquet as pq
import fastavro
import confluent_kafka

import numpy as np
from multiprocessing import Process, Queue, Value, Lock

from fink_client.configuration import load_credentials

from fink_client.consumer import (
    print_offsets,
    return_npartitions,
    return_partition_offset,
    return_last_offsets,
    get_schema_from_stream,
)
from fink_client.avro_utils import write_alerts
from fink_client.avro2arrow import avro_to_arrow
from fink_client.avro2arrow import create_partitioning
from fink_client.avro2arrow import avro_schema_to_arrow_schema

from fink_client.logger import get_fink_logger

_LOG = get_fink_logger("Fink", "WARNING")


def poll(
    process_id,
    nconsumers,
    queue,
    schema,
    kafka_config,
    rng,
    args,
    shared_counter,
    counter_lock,
):
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

    # infinite loop
    maxpoll = int(args.limit / nconsumers) if args.limit is not None else 1e10
    disable = not args.verbose

    poll_number = 0
    pbar = None
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

        # One bar per consumer
        if pbar is None:
            pbar = trange(
                partition["lag"],
                position=process_id,
                initial=initial,
                colour="#F5622E",
                unit="alerts",
                disable=disable,
                desc="Consumer {}".format(partition["partition"]),
                bar_format="{desc}: {n:,} {unit} [{rate_fmt}{postfix}]",
            )

        if offset == initial:
            if partition["status"] < max_end_check:
                # After max_end_check time if no alerts added,
                # it is supposed finished
                queue.put({
                    "partition": partition["partition"],
                    "offset": partition["offset"],
                    "status": partition["status"] + 1,
                    "lag": partition["lag"],
                })
        else:
            poll_number = initial
            try:
                while poll_number < maxpoll:
                    msgs = consumer.consume(args.batchsize, args.maxtimeout)
                    # Decode the message
                    if msgs is not None:
                        if len(msgs) == 0:
                            _LOG.info(
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
                                "lag": partition["lag"],
                            })
                            break

                        records = [
                            fastavro.schemaless_reader(io.BytesIO(msg.value()), schema)
                            for msg in msgs
                        ]

                        if len(records) == 0:
                            break

                        part_num = rng.randint(0, 1e9)
                        if args.outformat == "parquet":
                            table, arrow_schema = avro_to_arrow(schema, records)

                            # In-place partitioning
                            table, arrow_schema, partitioning = create_partitioning(
                                table=table,
                                arrow_schema=arrow_schema,
                                partitionby=args.partitionby,
                                survey=args.survey,
                            )

                            pq.write_to_dataset(
                                table,
                                args.outdir,
                                schema=arrow_schema,
                                basename_template="part-{}-{{i}}-{}.parquet".format(
                                    process_id, part_num
                                ),
                                partition_cols=partitioning,
                                existing_data_behavior="overwrite_or_ignore",
                            )
                        elif args.outformat == "avro":
                            write_alerts(
                                records,
                                schema,
                                root_path=args.outdir,
                                filename="part-{}-{}.avro".format(process_id, part_num),
                            )

                        poll_number += len(msgs)
                        pbar.update(len(msgs))
                        with counter_lock:
                            shared_counter.value += len(msgs)

                        if len(msgs) < args.batchsize:
                            queue.put({
                                "partition": partition["partition"],
                                "offset": poll_number,
                                "status": 0,
                                "lag": partition["lag"],
                            })
                            break
                    else:
                        _LOG.info(
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
        "-survey",
        type=str,
        required=True,
        help="Survey name among ztf or lsst. Note that each survey will have its own configuration file.",
    )
    parser.add_argument(
        "-topic",
        type=str,
        required=True,
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
        "-outformat",
        type=str,
        default="parquet",
        help="Output alert format. Choose among: parquet, avro. Default is parquet.",
    )
    parser.add_argument(
        "-partitionby",
        type=str,
        default=None,
        help="""
If specified, partition data when writing alerts on disk. Available options:
- `time`: year=YYYY/month=MM/day=DD (ztf and lsst)
- `finkclass`: finkclass=CLASS (ztf only)
- `tnsclass`: tnsclass=CLASS (ztf only)
- `classId`: classId=CLASSID (ELASTiCC only)
Default is None, that is no partitioning is applied (all parquet files in the `outdir` folder).
""",
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
        help="Number of partitions for the topic in the distant Kafka cluster. Do not change unless you know what your are doing. Default is 10 (Fink Kafka cluster)",
    )
    parser.add_argument(
        "--restart_from_beginning",
        action="store_true",
        help="If specified, restart downloading from the 1st alert in the stream. Default is False.",
    )
    parser.add_argument(
        "--dump_schemas",
        action="store_true",
        help="If specified, save the avro & arrow schemas on disk (json file)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="If specified, print on screen information about the consuming.",
    )
    args = parser.parse_args(None)

    if args.partitionby is not None and args.partitionby not in [
        "time",
        "finkclass",
        "tnsclass",
        "classId",
    ]:
        _LOG.error(
            "{} is an unknown partitioning. `-partitionby` should be in ['time', 'finkclass', 'tnsclass', 'classId']".format(
                args.partitionby
            )
        )
        sys.exit()

    if (
        args.partitionby
        in [
            "finkclass",
            "tnsclass",
            "classId",
        ]
        and args.survey == "lsst"
    ):
        _LOG.error(
            "{} is not available for lsst. No partitioning or `-partitionby=time` are allowed.".format(
                args.partitionby
            )
        )
        sys.exit()

    if not (args.topic.startswith("ftransfer") or args.topic.startswith("fxmatch")):
        msg = """
{} is not a valid topic name.
Topic name must start with `ftransfer_` or `fxmatch_`.
Check the webpage on which you submit the job,
and open the tab `Get your data` to retrieve the topic.
        """.format(args.topic)
        _LOG.error(msg)
        sys.exit()

    assert args.outformat in ["parquet", "avro"], (
        "-outformat must be one of parquet, avro. {} is not allowed.".format(
            args.outformat
        )
    )

    # load user configuration
    conf = load_credentials(survey=args.survey)

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
        offsets, lags = print_offsets(
            kafka_config,
            args.topic,
            args.maxtimeout,
            verbose=False,
            hide_empty_partition=False,
        )
        # All offsets, commited or not
        args.total_lag = sum(lags) + sum(offsets)
        args.total_offset = 0
        offsets = [0 for _ in range(args.number_partitions)]
    else:
        offsets, lags = print_offsets(
            kafka_config, args.topic, args.maxtimeout, hide_empty_partition=False
        )
        args.total_lag = sum(lags)
        args.total_offset = sum(offsets)
        offsets = return_last_offsets(kafka_config, args.topic)
        if args.total_lag == 0:
            _LOG.info("All alerts have been polled. Exiting.")
            sys.exit()

    args.topic_size = args.total_lag + args.total_offset

    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir, exist_ok=True)

    if (args.limit is not None) and (args.limit < args.batchsize):
        args.batchsize = args.limit

    avro_schema = get_schema_from_stream(kafka_config, args.topic, args.maxtimeout)
    if avro_schema is None:
        # TBD: raise error
        _LOG.info(
            "No schema found -- wait a few seconds and relaunch. If the error persists, maybe the queue is empty (i.e. your query produced no results)."
        )
        sys.exit()

    if args.dump_schemas:
        avro_filename = "avro_schema_{}.json".format(args.topic)
        with open(avro_filename, "w") as json_file:
            json.dump(avro_schema, json_file, sort_keys=True, indent=4)

        arrow_filename = "arrow_schema_{}.metadata".format(args.topic)
        pq.write_metadata(avro_schema_to_arrow_schema(avro_schema), arrow_filename)

    nbpart = return_npartitions(args.topic, kafka_config)
    _LOG.info("Number of partitions for topic {}: {}".format(args.topic, nbpart))
    available = Queue()
    # Queue loading
    for key in range(nbpart):
        available.put({
            "partition": key,
            "offset": offsets[key],
            "lag": lags[key],
            "status": 0,
        })

    # Initialize shared counter
    shared_counter = Value("i", 0)  # 'i' = signed integer
    counter_lock = Lock()

    # Create progress bar
    pbar_common = tqdm(
        position=0,
        desc="Dowloading",
        colour="#F5622E",
        initial=args.total_offset,
        total=args.topic_size,
    )

    # Processes Creation
    random_state = 0
    rng = np.random.RandomState(random_state)
    procs = []
    for i in range(nconsumers):
        proc = Process(
            target=poll,
            args=(
                i + 1,
                nconsumers,
                available,
                avro_schema,
                kafka_config,
                rng,
                args,
                shared_counter,
                counter_lock,
            ),
        )
        procs.append(proc)
        proc.start()

    # Monitor progress in main process
    last_count = 0
    while any(proc.is_alive() for proc in procs):
        with counter_lock:
            current_count = shared_counter.value

        # Update progress bar with delta
        pbar_common.update(current_count - last_count)
        last_count = current_count
        time.sleep(0.1)  # Poll interval

    # Final update
    with counter_lock:
        pbar_common.update(shared_counter.value - last_count)

    pbar_common.close()

    for proc in procs:
        proc.join()

    print_offsets(kafka_config, args.topic, args.maxtimeout)


if __name__ == "__main__":
    main()
