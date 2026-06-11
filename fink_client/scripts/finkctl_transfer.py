#!/usr/bin/env python
# Copyright 2023-2026 AstroLab Software
# Author: Julien Peloton, Saikou Oumar BAH, Farid MAMAN
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
"""Kafka consumer to listen and archive Fink streams from the data transfer service.

Supports two modes, selected automatically from the topic name:

  - Normal transfer  (topic starts with ``ftransfer_`` or ``fxmatch_``):
    Reads schemaless Avro alerts and writes Parquet files, exactly like the
    former ``fink_datatransfer`` command.

  - AI transfer  (topic starts with ``fink_ai_``):
    Reads JSON predictions from the output topic, joins them with the
    original Avro alerts from the companion feed topic
    (``fink_ai_feed_<job_id>``), and writes enriched Parquet files.
    Credentials are taken from the same ``finkctl auth register`` config —
    no need to pass ``-servers`` explicitly.
"""

import sys
import os
import io
import copy
import psutil
import json
import time

from tqdm import trange, tqdm

import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import confluent_kafka

import numpy as np
from types import SimpleNamespace
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

_AI_TOPIC_PREFIX = "fink_ai_"
_AI_FEED_PREFIX = "fink_ai_feed_"


def _is_ai_topic(topic: str) -> bool:
    """Return True if topic is an AI output topic (not a feed or pre topic)."""
    return (
        topic.startswith(_AI_TOPIC_PREFIX)
        and not topic.startswith(_AI_FEED_PREFIX)
        and "fink_ai_pre_" not in topic
    )


def _fix_spark_schema(schema: dict) -> dict:
    """Wrap top-level plain-record fields in [record, null] unions.

    Spark's to_avro() writes a union discriminator byte for nullable struct
    fields but the published schema lists them as plain records. fastavro
    raises IndexError without this fix.
    """
    fixed = copy.deepcopy(schema)
    for field in fixed.get("fields", []):
        t = field.get("type")
        if isinstance(t, dict) and t.get("type") == "record":
            field["type"] = [t, "null"]
    return fixed


def _get_ai_schema(kafka_config: dict, feed_topic: str, maxtimeout: float):
    """Return parsed Avro schema for the feed topic, with Spark union fix applied."""
    raw = get_schema_from_stream(kafka_config, feed_topic, maxtimeout)
    if raw is None:
        return None
    # raw is already parsed by get_schema_from_stream — re-parse with the fix
    # by extracting the original dict and re-parsing
    try:
        return fastavro.parse_schema(_fix_spark_schema(raw))
    except Exception:
        return raw


def _flatten(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(_flatten(v, new_key, sep))
        else:
            items[new_key] = v
    return items


def _read_predictions(kafka_config: dict, topic: str, batchsize: int, maxtimeout: float, limit, verbose: bool) -> dict:
    """Return dict  candid → {prediction, predictions, bridge}."""
    consumer = confluent_kafka.Consumer(kafka_config)
    results = {}
    try:
        _, lags = print_offsets(kafka_config, topic, maxtimeout, verbose=False, hide_empty_partition=False)
        total = sum(lags)
        if total == 0:
            if verbose:
                print("No predictions found in output topic yet.")
            return results

        pbar = tqdm(
            total=limit if limit else total,
            desc="Predictions",
            colour="#F5622E",
            unit="alerts",
            bar_format="{desc}: {n:,}/{total:,} {unit} [{rate_fmt}{postfix}]",
            disable=not verbose,
        )

        consumer.subscribe([topic])
        n = 0
        while True:
            msgs = consumer.consume(num_messages=batchsize, timeout=maxtimeout)
            if not msgs:
                break
            for msg in msgs:
                if msg.error():
                    continue
                try:
                    rec = json.loads(msg.value())
                except (json.JSONDecodeError, Exception):
                    continue

                source = rec.get("source") or {}
                result = rec.get("result")

                if isinstance(result, dict):
                    preds = result.get("predictions") or []
                    if not isinstance(preds, list):
                        preds = [preds]
                elif isinstance(result, (int, float)):
                    preds = [result]
                elif isinstance(result, list):
                    preds = result
                else:
                    preds = []

                preds = [float(p) if p is not None else float("nan") for p in preds]
                candid = source.get("candid")
                if candid is not None:
                    results[int(candid)] = {
                        "prediction": preds[0] if preds else float("nan"),
                        "predictions": preds,
                        "bridge": str(rec.get("bridge") or ""),
                    }
                    n += 1
                    pbar.update(1)
            if limit and n >= limit:
                break
        pbar.close()
    finally:
        consumer.close()

    return results


def _read_alerts(kafka_config: dict, feed_topic: str, predictions: dict, batchsize: int, maxtimeout: float, verbose: bool) -> dict:
    """Return dict  candid → flattened alert fields."""
    alerts = {}

    _, lags = print_offsets(kafka_config, feed_topic, maxtimeout, verbose=False, hide_empty_partition=False)
    if sum(lags) == 0:
        if verbose:
            print(f"Feed topic empty or not found ({feed_topic}) — writing predictions only.")
        return alerts

    # reuse get_schema_from_stream + apply Spark fix
    avro_schema = _get_ai_schema(kafka_config, feed_topic, maxtimeout)
    if avro_schema is None and verbose:
        print(f"WARNING: no schema in {feed_topic}_schema — alerts may not decode")

    consumer = confluent_kafka.Consumer(kafka_config)
    try:
        consumer.subscribe([feed_topic])
        needed = set(predictions.keys())
        pbar = tqdm(
            total=len(needed),
            desc="Alerts     ",
            colour="#F5622E",
            unit="alerts",
            bar_format="{desc}: {n:,}/{total:,} {unit} [{rate_fmt}{postfix}]",
            disable=not verbose,
        )
        while needed:
            msgs = consumer.consume(num_messages=batchsize, timeout=maxtimeout)
            if not msgs:
                break
            for msg in msgs:
                if msg.error():
                    continue
                try:
                    if avro_schema is not None:
                        rec = fastavro.schemaless_reader(io.BytesIO(msg.value()), avro_schema)
                    else:
                        recs = list(fastavro.reader(io.BytesIO(msg.value())))
                        rec = recs[0] if recs else None
                    if rec is None:
                        continue
                    candid = rec.get("candid")
                    if candid is not None and int(candid) in needed:
                        alerts[int(candid)] = _flatten(rec)
                        needed.discard(int(candid))
                        pbar.update(1)
                except Exception:
                    continue
            if not needed:
                break
        pbar.close()
    finally:
        consumer.close()

    return alerts


def _join_and_write_ai(predictions: dict, alerts: dict, args):
    """Join predictions with alert fields and write Parquet, same layout as normal transfer."""
    if not predictions:
        print("No predictions to write.")
        return

    rows = []
    for candid, pred in predictions.items():
        row = {"candid": candid, **pred}
        if candid in alerts:
            row.update({k: v for k, v in alerts[candid].items() if k != "candid"})
        rows.append(row)

    all_keys = list(rows[0].keys())
    for row in rows[1:]:
        for k in row:
            if k not in all_keys:
                all_keys.append(k)

    columns = {k: [row.get(k) for row in rows] for k in all_keys}

    arrays = {}
    for k, vals in columns.items():
        if k == "predictions":
            arrays[k] = pa.array(vals, type=pa.list_(pa.float64()))
        else:
            try:
                arrays[k] = pa.array(vals)
            except Exception:
                arrays[k] = pa.array([str(v) if v is not None else None for v in vals])

    table = pa.table(arrays)
    arrow_schema = table.schema

    # Apply same partitioning as normal transfer if requested
    partitioning = None
    if args.partitionby is not None:
        table, arrow_schema, partitioning = create_partitioning(
            table=table,
            arrow_schema=arrow_schema,
            partitionby=args.partitionby,
            survey=args.survey,
        )

    os.makedirs(args.outdir, exist_ok=True)
    rng = np.random.RandomState(42)
    pq.write_to_dataset(
        table,
        args.outdir,
        schema=arrow_schema,
        basename_template="part-0-{{i}}-{}.parquet".format(rng.randint(0, int(1e9))),
        partition_cols=partitioning,
        existing_data_behavior="overwrite_or_ignore",
    )

    if args.verbose:
        print(f"\nDone — {len(rows):,} rows written to '{args.outdir}/'")
        print(f"Columns ({len(all_keys)}): {', '.join(all_keys[:10])}" + (" ..." if len(all_keys) > 10 else ""))
        print("\nRead your results:")
        print(f"  import pandas as pd")
        print(f"  df = pd.read_parquet('{args.outdir}/', dtype_backend='pyarrow')")


def _transfer_ai(args, conf):
    """Retrieve AI inference results and join with original alerts."""
    kafka_config = {
        "bootstrap.servers": conf["servers"],
        "group.id": conf.get("groupid") or conf.get("group_id"),
        "auto.offset.reset": "earliest",
    }
    feed_topic = args.topic.replace(_AI_TOPIC_PREFIX, _AI_FEED_PREFIX, 1)

    # Same partition table display as normal transfer
    offsets, lags = print_offsets(kafka_config, args.topic, args.maxtimeout, hide_empty_partition=False)

    # Same behavior as normal transfer: exit if already fully consumed
    if sum(lags) == 0:
        _LOG.info("All predictions have been polled. Exiting.")
        sys.exit()

    predictions = _read_predictions(
        kafka_config, args.topic,
        args.batchsize, args.maxtimeout, args.limit, args.verbose,
    )
    alerts = _read_alerts(
        kafka_config, feed_topic,
        predictions, args.batchsize, args.maxtimeout, args.verbose,
    )
    _join_and_write_ai(predictions, alerts, args)

    # Final state — same as normal transfer
    print_offsets(kafka_config, args.topic, args.maxtimeout, hide_empty_partition=False)


def poll(
    process_id,
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
    maxpoll = int(args.limit / args.nconsumers) if args.limit is not None else 1e10

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
                disable=not args.verbose,
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
                    msgs = []
                    for _ in range(args.batchsize):
                        msg = consumer.poll(args.maxtimeout)
                        if msg is not None:
                            msgs.append(msg)
                        else:
                            break
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


def transfer_(
    survey,
    topic,
    limit,
    outdir,
    outformat,
    partitionby,
    batchsize,
    nconsumers_,
    maxtimeout,
    number_partitions,
    restart_from_beginning,
    dump_schemas,
    verbose,
):
    """ """
    dict_args = {}
    dict_args["survey"] = survey
    dict_args["topic"] = topic
    dict_args["limit"] = limit
    dict_args["outdir"] = outdir
    dict_args["outformat"] = outformat
    dict_args["partitionby"] = partitionby
    dict_args["batchsize"] = batchsize
    dict_args["nconsumers"] = nconsumers_
    dict_args["maxtimeout"] = maxtimeout
    dict_args["restart_from_beginning"] = restart_from_beginning
    dict_args["dump_schemas"] = dump_schemas
    dict_args["verbose"] = verbose

    args = SimpleNamespace(**dict_args)

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

    valid_prefixes = ("ftransfer", "fxmatch", _AI_TOPIC_PREFIX)
    if not args.topic.startswith(valid_prefixes):
        msg = """
{} is not a valid topic name.
Topic name must start with `ftransfer_`, `fxmatch_`, or `fink_ai_`.
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

    # AI topics: delegate to the AI transfer path and return early
    if _is_ai_topic(args.topic):
        _transfer_ai(args, conf)
        return

    # Number of consumers to use
    if nconsumers_ == -1:
        args.nconsumers = psutil.cpu_count(logical=True)
    else:
        args.nconsumers = nconsumers_

    kafka_config = {
        "bootstrap.servers": conf["servers"],
        "group.id": conf.get("groupid") or conf.get("group_id"),
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
        offsets = [0 for _ in range(number_partitions)]
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
    for procid in range(args.nconsumers):
        proc = Process(
            target=poll,
            args=(
                procid + 1,
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
