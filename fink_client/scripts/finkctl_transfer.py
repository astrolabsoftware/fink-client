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
    original Avro alerts from the feed topic, and writes enriched Parquet
    files with the same layout as a classic transfer. Credentials are
    taken from the same ``finkctl auth register`` config — no need to
    pass ``-servers`` explicitly.

    Alerts and predictions are first downloaded to a temporary folder,
    following the AI job until it is done, then joined with polars. The
    temporary folder is removed at the end; relaunching resumes the transfer.
"""

import sys
import os
import io
import psutil
import json
import shutil
import time

from tqdm import trange, tqdm

import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import confluent_kafka

import numpy as np
import polars as pl
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
# Stop following the AI job after this many seconds without a new prediction
_AI_IDLE_TIMEOUT = 300


def _is_ai_topic(topic: str) -> bool:
    """Return True if topic is an AI output topic (not a feed or pre topic)."""
    return (
        topic.startswith(_AI_TOPIC_PREFIX)
        and not topic.startswith(_AI_FEED_PREFIX)
        and "fink_ai_pre_" not in topic
    )


def _topic_exists(kafka_config: dict, topic: str, timeout: float) -> bool:
    """Return True if `topic` exists on the broker, without raising."""
    consumer = confluent_kafka.Consumer(kafka_config)
    try:
        metadata = consumer.list_topics(topic, timeout=timeout)
        return metadata.topics[topic].error is None
    except Exception:
        return False
    finally:
        consumer.close()


def _resolve_feed_topic(kafka_config: dict, output_topic: str, survey: str, timeout):
    """Return the topic holding the original alerts of an AI topic, or None.

    Tries `fink_ai_feed_<suffix>` first, then `ftransfer_<survey>_<suffix>`
    (K8S_ONLY_MODE portal deployments reuse that topic directly).
    """
    suffix = output_topic[len(_AI_TOPIC_PREFIX) :]
    for candidate in [_AI_FEED_PREFIX + suffix, f"ftransfer_{survey}_{suffix}"]:
        if _topic_exists(kafka_config, candidate, timeout):
            return candidate
    return None


_PREDICTION_SCHEMA = pa.schema([
    ("candid", pa.int64()),
    ("model", pa.string()),
    ("prediction", pa.float64()),
])
_MODEL_PREDICTIONS = pa.field(
    "model_predictions",
    pa.list_(pa.struct([("model", pa.string()), ("prediction", pa.float64())])),
)


def _assign(consumer, topic: str, timeout: float, start: dict) -> dict:
    """Assign all partitions of `topic`, from `start[partition]` to their end.

    Returns
    -------
    ends: dict
        partition -> end offset, for partitions with messages left to read
    """
    partitions = consumer.list_topics(topic, timeout=timeout).topics[topic].partitions
    assignment, ends = [], {}
    for p in partitions:
        low, high = consumer.get_watermark_offsets(
            confluent_kafka.TopicPartition(topic, p), timeout=timeout
        )
        first = max(start.get(p, low), low)
        assignment.append(confluent_kafka.TopicPartition(topic, p, first))
        if first < high:
            ends[p] = high
    consumer.assign(assignment)
    return ends


def _topic_size(consumer, topic: str, timeout: float) -> int:
    """Return the number of messages in `topic`."""
    partitions = consumer.list_topics(topic, timeout=timeout).topics[topic].partitions
    return sum(
        high - low
        for low, high in (
            consumer.get_watermark_offsets(
                confluent_kafka.TopicPartition(topic, p), timeout=timeout
            )
            for p in partitions
        )
    )


def _decode_prediction(msg):
    """Return a prediction message as a row, or None if malformed."""
    try:
        rec = json.loads(msg.value())
    except (TypeError, ValueError):
        _LOG.warning("Skipped malformed prediction (offset %s)", msg.offset())
        return None
    candid = (rec.get("source") or {}).get("candid")
    if candid is None:
        return None
    result = rec.get("result")
    pred = result.get("predictions") if isinstance(result, dict) else result
    if isinstance(pred, list):
        pred = pred[0] if pred else None
    return {
        "candid": int(candid),
        "model": str(rec.get("bridge") or ""),
        "prediction": float("nan") if pred is None else float(pred),
    }


class _Spool:
    """Messages of a topic stored on disk, one Parquet file per chunk and partition.

    File names hold the offsets they contain (`p<partition>-<first>-<last>`):
    the folder is the state of the download, and a relaunch resumes from it.
    """

    def __init__(self, path: str, schema: pa.Schema):
        os.makedirs(path, exist_ok=True)
        self.path = path
        self.schema = schema
        self.next = {}  # partition -> next offset to download
        self.count = 0
        self.buffers = {}  # partition -> (first offset, last offset, rows)
        for name in os.listdir(path):
            if name.endswith(".parquet"):
                partition, first, last = map(int, name[1:-8].split("-"))
                self.next[partition] = max(self.next.get(partition, 0), last + 1)
                self.count += last - first + 1

    @property
    def buffered(self) -> int:
        return sum(len(rows) for _, _, rows in self.buffers.values())

    def add(self, msg, row: dict):
        partition, offset = msg.partition(), msg.offset()
        first, _, rows = self.buffers.get(partition, (offset, offset, []))
        rows.append(row)
        self.buffers[partition] = (first, offset, rows)

    def flush(self):
        """Write the buffered rows, atomically."""
        for partition, (first, last, rows) in self.buffers.items():
            path = os.path.join(
                self.path, "p{}-{:012d}-{:012d}.parquet".format(partition, first, last)
            )
            pq.write_table(
                pa.Table.from_pylist(rows, schema=self.schema), path + ".tmp"
            )
            os.replace(path + ".tmp", path)
            self.next[partition] = last + 1
            self.count += len(rows)
        self.buffers = {}


def _download(args, kafka_config, feed_topic, avro_schema, alerts, predictions):
    """Download the alerts and the predictions to disk.

    Predictions are produced over time by the AI job: they are downloaded
    until there is one per alert, or until none arrives for
    `_AI_IDLE_TIMEOUT` seconds.
    """
    alert_consumer = confluent_kafka.Consumer(kafka_config)
    prediction_consumer = confluent_kafka.Consumer(kafka_config)
    total = _topic_size(alert_consumer, feed_topic, args.maxtimeout)
    limit = min(args.limit or total, total)
    alert_bar = tqdm(
        desc="Alerts     ",
        total=limit,
        initial=alerts.count,
        unit="alerts",
        colour="#F5622E",
        position=0,
    )
    prediction_bar = tqdm(
        desc="Predictions",
        total=limit,
        initial=predictions.count,
        unit="alerts",
        colour="#15284F",
        position=1,
    )
    try:
        alert_ends = _assign(alert_consumer, feed_topic, args.maxtimeout, alerts.next)

        # Predictions resume from the files on disk, or from the committed offsets
        committed = prediction_consumer.committed(
            [
                confluent_kafka.TopicPartition(args.topic, p)
                for p in alert_consumer
                .list_topics(args.topic, timeout=args.maxtimeout)
                .topics[args.topic]
                .partitions
            ],
            timeout=args.maxtimeout,
        )
        start = {tp.partition: tp.offset for tp in committed}
        for partition, offset in predictions.next.items():
            start[partition] = max(offset, start.get(partition, 0))
        _assign(prediction_consumer, args.topic, args.maxtimeout, start)

        models, last_seen, last_flush = set(), time.time(), time.time()
        while True:
            downloading = alert_ends and alerts.count + alerts.buffered < limit
            if downloading:
                for msg in alert_consumer.consume(args.batchsize, timeout=1):
                    end = alert_ends.get(msg.partition())
                    if msg.error() or end is None or msg.offset() >= end:
                        continue
                    alert = fastavro.schemaless_reader(
                        io.BytesIO(msg.value()), avro_schema
                    )
                    alerts.add(msg, alert)
                    alert_bar.update(1)
                    if msg.offset() + 1 >= end:
                        del alert_ends[msg.partition()]
                if alerts.buffered >= args.batchsize or not alert_ends:
                    alerts.flush()

            msgs = prediction_consumer.consume(
                10 * args.batchsize, timeout=0 if downloading else args.maxtimeout
            )
            for msg in msgs:
                row = None if msg.error() else _decode_prediction(msg)
                if row is not None:
                    predictions.add(msg, row)
                    models.add(row["model"])
            if msgs:
                last_seen = time.time()
                prediction_bar.total = max(
                    prediction_bar.total,
                    limit * len(models),
                    prediction_bar.n + len(msgs),
                )
                prediction_bar.update(len(msgs))
            if predictions.buffered >= 10 * args.batchsize or (
                predictions.buffered and time.time() - last_flush > args.maxtimeout
            ):
                predictions.flush()
                last_flush = time.time()

            if downloading:
                continue
            alerts.flush()
            if predictions.count + predictions.buffered >= limit * max(len(models), 1):
                break
            if time.time() - last_seen > _AI_IDLE_TIMEOUT:
                _LOG.warning(
                    "No new prediction for {} min, stopping the download.".format(
                        _AI_IDLE_TIMEOUT // 60
                    )
                )
                break
            prediction_bar.set_postfix_str("waiting for the AI job")
        predictions.flush()
    finally:
        alert_bar.close()
        prediction_bar.close()
        alert_consumer.close()
        prediction_consumer.close()


def _join_chunk(table: pa.Table, grouped: pl.DataFrame, name: str, args) -> int:
    """Join a chunk of alerts with their predictions, and write it.

    Alerts stay in Arrow to keep their schema: polars only matches the keys.
    Returns the number of alerts written.
    """
    match = (
        pl
        .from_arrow(table.select(["candid"]))
        .with_row_index("row")
        .join(grouped, on="candid", how="inner", maintain_order="left")
    )
    joined = table.take(match["row"].to_arrow()).append_column(
        _MODEL_PREDICTIONS,
        match["model_predictions"].to_arrow().cast(_MODEL_PREDICTIONS.type),
    )
    joined, arrow_schema, partitioning = create_partitioning(
        table=joined,
        arrow_schema=joined.schema,
        partitionby=args.partitionby,
        survey=args.survey,
    )
    pq.write_to_dataset(
        joined,
        args.outdir,
        schema=arrow_schema,
        basename_template="part-ai-" + name + "-{i}.parquet",
        partition_cols=partitioning,
        existing_data_behavior="overwrite_or_ignore",
    )
    return joined.num_rows


def _join(args, alerts: _Spool, predictions: _Spool) -> int:
    """Join the downloaded alerts with their predictions, and write the result.

    Predictions are grouped per alert with polars, then alerts are joined by
    chunks of `10 * batchsize`. Output file names derive from the input ones:
    running the join again overwrites, never duplicates.

    Returns the number of alerts written.
    """
    files = [f for f in os.listdir(predictions.path) if f.endswith(".parquet")]
    if not files:
        return 0
    grouped = (
        pl
        .scan_parquet([os.path.join(predictions.path, f) for f in files])
        .unique(["candid", "model"], keep="last")
        .group_by("candid")
        .agg(pl.struct("model", "prediction").alias("model_predictions"))
        .collect()
    )

    nwritten, chunk = 0, []
    bar = tqdm(desc="Join       ", total=alerts.count, unit="alerts", colour="#F5622E")
    names = sorted(f for f in os.listdir(alerts.path) if f.endswith(".parquet"))
    for i, name in enumerate(names):
        chunk.append(pq.read_table(os.path.join(alerts.path, name)))
        if sum(t.num_rows for t in chunk) < 10 * args.batchsize and i < len(names) - 1:
            continue
        table = pa.concat_tables(chunk)
        nwritten += _join_chunk(table, grouped, name[:-8], args)
        bar.update(table.num_rows)
        chunk = []
    bar.close()
    return nwritten


def _transfer_ai(args, kafka_config):
    """Download an AI topic and its alerts, join them, and write Parquet.

    1. Alerts and predictions are downloaded to a temporary folder in
       `outdir`, following the AI job until it is done.
    2. Predictions are joined with their alerts, and written with the same
       layout as a classic transfer.
    3. Predictions are committed, and the temporary folder is removed.

    Relaunching the command after an interruption resumes where it stopped.
    """
    if args.verbose:
        get_fink_logger("Fink", "INFO")

    tmpdir = os.path.join(args.outdir, ".tmp")
    _, lags = print_offsets(
        kafka_config,
        args.topic,
        args.maxtimeout,
        verbose=False,
        hide_empty_partition=False,
    )
    if sum(lags) == 0 and not os.path.isdir(tmpdir):
        _LOG.info("All predictions have been polled. Exiting.")
        sys.exit()

    feed_topic = _resolve_feed_topic(
        kafka_config, args.topic, args.survey, args.maxtimeout
    )
    if feed_topic is None:
        _LOG.error("No alert topic found for {}.".format(args.topic))
        sys.exit()

    # Dedicated group, never committed: the schema is always read from the start
    schema_config = dict(
        kafka_config,
        **{
            "group.id": kafka_config["group.id"] + "_schema",
            "enable.auto.commit": False,
        },
    )
    avro_schema = get_schema_from_stream(schema_config, feed_topic, args.maxtimeout)
    if avro_schema is None:
        _LOG.error(
            "No schema found for {} -- relaunch in a few seconds.".format(feed_topic)
        )
        sys.exit()

    kafka_config = dict(kafka_config, **{"enable.auto.commit": False})
    alerts = _Spool(
        os.path.join(tmpdir, "alerts"), avro_schema_to_arrow_schema(avro_schema)
    )
    predictions = _Spool(os.path.join(tmpdir, "predictions"), _PREDICTION_SCHEMA)
    try:
        _download(args, kafka_config, feed_topic, avro_schema, alerts, predictions)
        nwritten = _join(args, alerts, predictions)
    except KeyboardInterrupt:
        sys.stderr.write("\n%% Aborted by user -- relaunch to resume\n")
        return

    # Everything is written: commit the predictions, and clean up
    consumer = confluent_kafka.Consumer(kafka_config)
    consumer.commit(
        offsets=[
            confluent_kafka.TopicPartition(args.topic, partition, offset)
            for partition, offset in predictions.next.items()
        ],
        asynchronous=False,
    )
    consumer.close()
    shutil.rmtree(tmpdir)

    if nwritten < alerts.count:
        _LOG.warning(
            "{:,} alerts have no prediction yet: relaunch later to get them.".format(
                alerts.count - nwritten
            )
        )
    _LOG.info("{:,} alerts written to {}/".format(nwritten, args.outdir))


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

    valid_prefixes = ("ftransfer_", "fxmatch_", _AI_TOPIC_PREFIX)
    if not args.topic.startswith(valid_prefixes):
        msg = """
{} is not a valid topic name.
Topic name must start with `ftransfer_`, `fxmatch_`, or `fink_ai_`.
Check the webpage on which you submit the job,
and open the tab `Get your data` to retrieve the topic.
        """.format(args.topic)
        _LOG.error(msg)
        sys.exit()

    assert args.outformat in [
        "parquet",
        "avro",
    ], "-outformat must be one of parquet, avro. {} is not allowed.".format(
        args.outformat
    )
    # load user configuration
    conf = load_credentials(survey=args.survey)

    # Time to wait before polling again if no alerts
    if args.maxtimeout is None:
        args.maxtimeout = conf["maxtimeout"]

    kafka_config = {
        "bootstrap.servers": conf["servers"],
        "group.id": conf.get("groupid") or conf.get("group_id"),
        "auto.offset.reset": "earliest",
    }

    # AI topics: delegate to the AI transfer path and return early
    if _is_ai_topic(args.topic):
        _transfer_ai(args, kafka_config)
        return

    # Number of consumers to use
    if nconsumers_ == -1:
        args.nconsumers = psutil.cpu_count(logical=True)
    else:
        args.nconsumers = nconsumers_

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
