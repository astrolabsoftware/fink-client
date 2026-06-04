#!/usr/bin/env python
# Copyright 2026 AstroLab Software
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
"""fink-client CLI"""

from fink_client import __version__
from fink_client.configuration import load_credentials
from fink_client.configuration import add_topic
from fink_client.configuration import remove_topic
import rich_click as click


click.rich_click.THEME = "red1-box"

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
    help="""Fink client to interact with various Fink services (version {}).

    You can use no args or --help/-h at the top level and also for
    specific subcommands to get help.

    $ finkctl COMMAND

    $ finkctl COMMAND -h

    $ finkctl COMMAND --help
    """.format(__version__),
)
def cli():
    pass


@cli.group(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
)
def auth():
    """Configure the client connection for the different services."""
    pass


@click.option(
    "-survey",
    type=click.Choice(["ztf", "lsst"]),
    required=True,
    help="Survey name.",
)
@auth.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
def show(survey):
    """Show current credentials."""
    (
        click.secho(
            "If you see a wrong entry for {groupid, username, servers, maxtimeout}, run:",
            italic=True,
        ),
    )
    (click.secho("$ finkctl auth register", bold=True, italic=True),)
    (click.secho("If you see a wrong entry for a topic, run:", italic=True),)
    (click.secho("$ finkctl topic subscribe", bold=True, italic=True),)
    click.echo("")
    import yaml

    conf = load_credentials(survey=survey)
    print(yaml.dump(conf, default_flow_style=False))


@click.option(
    "--tmp",
    is_flag=True,
    help="If specified, register credentials in /tmp instead of ${HOME}/.finkclient/. Useful to debug without affecting current credentials.",
)
@click.option(
    "-maxtimeout",
    type=int,
    default=10,
    help="Timeout when polling the servers. Default is 10 seconds.",
)
@click.option(
    "-log_level",
    type=str,
    default="WARN",
    help="Level of verbosity. Default is WARN. Set to INFO or DEBUG to get more information",
)
@click.option(
    "-servers",
    type=str,
    required=True,
    help="Fink Kafka bootstrap server in the form name:port",
)
@click.option(
    "-groupid",
    type=str,
    required=True,
    help="group.id used for the authentication on the Kafka cluster",
)
@click.option(
    "-username",
    type=str,
    required=True,
    help="username used for the authentication on the Kafka cluster",
)
@click.option(
    "-survey",
    type=click.Choice(["ztf", "lsst"]),
    required=True,
    help="Survey name. Note that each survey will have its own configuration file.",
)
@auth.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
def register(survey, username, groupid, servers, log_level, maxtimeout, tmp):
    """Configure the client connection for the different services.

    You should run `register` for all surveys you are using.
    """
    from fink_client.scripts.finkctl_register import register_

    register_(survey, username, groupid, servers, log_level, maxtimeout, tmp)


@cli.group(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
)
def topic():
    """List, subscribe, or remove topics for the Livestream service."""
    pass


@topic.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
@click.option(
    "-survey",
    type=click.Choice(["ztf", "lsst"]),
    required=True,
    help="Survey name.",
)
@click.option(
    "-name",
    type=str,
    required=True,
    help="Kafka topic name to subscribe to. Should start with fink_",
)
@click.option(
    "-telegram_token",
    type=str,
    help="Token to use to redirect to a Telegram channel.",
)
@click.option(
    "-telegram_channel",
    type=str,
    help="Channel name in Telegram to redirect alerts to. The channel must exist.",
)
def subscribe(survey, name, telegram_token, telegram_channel):
    """Subscribe to a new topic for the Livestream service

    Examples
    --------
    $ finkctl topic subscribe -survey lsst -name fink_in_tns_lsst -telegram_token $TOKEN -telegram_channel "@fink_tns"
    """
    add_topic(survey, name, telegram_token, telegram_channel)


@topic.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
@click.option(
    "-survey",
    type=click.Choice(["ztf", "lsst"]),
    required=True,
    help="Survey name.",
)
@click.option(
    "-name",
    type=str,
    required=True,
    help="Kafka topic name. Should start with fink_",
)
def remove(survey, name):
    """Remove a topic in your configuration for the Livestream service."""
    remove_topic(survey, name)


@topic.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="""
    More information at

    - ZTF: https://doc.ztf.fink-broker.org/en/latest/broker/filters/

    - LSST: https://doc.lsst.fink-broker.org/science/filters/
    """,
    no_args_is_help=True,
)
@click.option(
    "-survey",
    type=click.Choice(["ztf", "lsst"]),
    required=True,
    help="Survey name.",
)
def list(survey):
    """List topics for the Livestream service."""
    # load user configuration
    from tabulate import tabulate

    conf = load_credentials(survey=survey)

    # TODO: Extract subscribed
    if survey == "ztf":
        from fink_client.consumer import AlertConsumer

        # FIXME: Add endpoint like for LSST
        conf = load_credentials(survey=survey)
        config = {
            "bootstrap.servers": conf["servers"],
            "group.id": conf["groupid"],
        }
        consumer = AlertConsumer(
            topics=[],
            config=config,
            survey=conf["survey"],
            schema_path=None,
            dump_schema=False,
            on_assign=None,
        )
        topics = consumer.available_topics(service="livestream")
        print(
            tabulate(
                [[i, "NO"] for i in topics],
                [f"Fink/{survey.upper()} topics", "Subscribed"],
                tablefmt="pretty",
            )
        )
        consumer.close()
    if survey == "lsst":
        import requests

        r = requests.get("https://api.lsst.fink-portal.org/api/v1/tags")
        out = r.json()
        print(
            tabulate(
                [
                    ["fink_" + k + "_lsst", v["description"], "NO"]
                    for k, v in out.items()
                ],
                [f"Fink/{survey.upper()} topics", "Description", "Subscribed"],
                tablefmt="grid",
            )
        )


@cli.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
@click.option(
    "-survey",
    type=click.Choice(["ztf", "lsst"]),
    required=True,
    help="Survey name.",
)
@click.option(
    "-limit",
    type=int,
    default=None,
    help="If specified, download only `limit` alerts. Default is None.",
)
@click.option(
    "-start_at",
    type=str,
    default="",
    help=r"If specified, reset offsets to 0 (`earliest`) or empty queue (`latest`).",
)
@click.option(
    "-outdir",
    type=str,
    default=".",
    help="Folder to store incoming alerts if --save is set. It must exist.",
)
@click.option(
    "-ext_schema",
    type=str,
    default=None,
    help="Path to Avro schema to decode the incoming alerts. Default is None (version taken from each alert)",
)
@click.option(
    "--display_statistics",
    is_flag=True,
    help="If specified, print on screen information about queues, and exit.",
)
@click.option(
    "--display",
    is_flag=True,
    help="If specified, print on screen information about incoming alert.",
)
@click.option(
    "--save",
    is_flag=True,
    help="If specified, save alert data on disk (Avro). See also -outdir.",
)
@click.option(
    "--telegram",
    is_flag=True,
    help="If specified, redirect alerts on a Telegram channel.",
)
@click.option(
    "--slack",
    is_flag=True,
    help="If specified, redirect alerts on a Slack channel.",
)
@click.option(
    "--dump_schema",
    is_flag=True,
    help="If specified, save the schema on disk (json file) before polling.",
)
def stream(
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
    """Poll alerts from the Fink Livestream service and save or redirect alerts using Fink bots

    The list of available topics can be seen from `finkctl topic list`.
    """
    from fink_client.scripts.finkctl_stream import stream_

    stream_(
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
    )


@cli.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
@click.option(
    "-survey",
    type=click.Choice(["ztf", "lsst"]),
    required=True,
    help="Survey name.",
)
@click.option(
    "-topic",
    type=str,
    required=True,
    help="Topic name for the stream that contains the data.",
)
@click.option(
    "-outdir",
    type=str,
    default=".",
    help="Folder to store incoming alerts. It will be created if it does not exist. Default is current directory.",
)
@click.option(
    "-outformat",
    type=click.Choice(["parquet", "avro"]),
    default="parquet",
    help="Output alert format. Default is parquet.",
)
@click.option(
    "-partitionby",
    type=click.Choice([None, "time", "finkclass", "tnsclass", "classId"]),
    default=None,
    help="""
If specified, partition data when writing alerts on disk. Available options:

- None: no partitioning (ztf and lsst)

- time: year=YYYY/month=MM/day=DD (ztf and lsst)

- finkclass: finkclass=CLASS (ztf only)

- tnsclass: tnsclass=CLASS (ztf only)

- classId: classId=CLASSID (ELASTiCC only)

Default is None, that is no partitioning is applied (all parquet files in the `outdir` folder).
""",
)
@click.option(
    "-limit",
    type=int,
    default=None,
    help="If specified, download only `limit` alerts from the stream, otherwise download all alerts.",
)
@click.option(
    "-batchsize",
    type=int,
    default=100,
    help="Maximum number of alert within the `maxtimeout` (see conf). Default is 100 alerts.",
)
@click.option(
    "-nconsumers",
    type=int,
    default=-1,
    help="Number of parallel consumer to use. Default (-1) is the number of logical CPUs in the system.",
)
@click.option(
    "-maxtimeout",
    type=float,
    default=None,
    help="Overwrite the default timeout (in seconds) from user configuration.",
)
@click.option(
    "-number_partitions",
    type=int,
    default=10,
    help="Number of partitions for the topic in the distant Kafka cluster. Do not change unless you know what your are doing. Default is 10 (Fink Kafka cluster)",
)
@click.option(
    "--restart_from_beginning",
    is_flag=True,
    help="If specified, restart downloading from the 1st alert in the stream.",
)
@click.option(
    "--dump_schemas",
    is_flag=True,
    help="If specified, save the avro & arrow schemas on disk (json file)",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="If specified, print on screen information about the consuming.",
)
def transfer(
    survey,
    topic,
    limit,
    outdir,
    outformat,
    partitionby,
    batchsize,
    nconsumers,
    maxtimeout,
    number_partitions,
    restart_from_beginning,
    dump_schemas,
    verbose,
):
    """Archive Fink streams from the Fink Data Transfer service."""
    from fink_client.scripts.finkctl_transfer import transfer_

    transfer_(
        survey,
        topic,
        limit,
        outdir,
        outformat,
        partitionby,
        batchsize,
        nconsumers,
        maxtimeout,
        number_partitions,
        restart_from_beginning,
        dump_schemas,
        verbose,
    )


@cli.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
def search():
    """Search for alerts in the Fink database."""
    click.echo("Not yet available")
