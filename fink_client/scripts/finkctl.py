import rich_click as click

from fink_client.scripts.finkctl_register import register_

click.rich_click.THEME = "red1-nu"

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
def cli():
    """Fink client to interact with various Fink services.

    You can try using no args or --help/-h at the top level and also for
    specific subcommands. E.g. these are equivalent commands showing help in `register`

    $ finkctl register

    $ finkctl register -h

    $ finkctl register --help
    """
    pass


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
@cli.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at https://fink-broker.org/",
    no_args_is_help=True,
)
def register(survey, username, group_id, servers, log_level, maxtimeout, tmp):
    """Configure the client connection for the different services.

    You should run `register` for all surveys you are using.
    """
    register_(survey, username, group_id, servers, topic, log_level, maxtimeout, tmp)


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
    """Subscribe to a new topic for the Livestream service"""
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
    help="Kafka topic name. Should start with fink_",
)
def remove(survey, name):
    """Remove a topic for the Livestream service."""
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
    "--available",
    is_flag=True,
    help="If specified, list all available topics",
)
@click.option(
    "--subscribed",
    is_flag=True,
    help="If specified, list all subscribed topics",
)
def list(survey, available, subscribed):
    """List topics for the Livestream service."""
    pass
