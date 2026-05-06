#import click
import rich_click as click

click.rich_click.THEME = "red1-modern"

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@click.option(
    "-survey",
    type=str,
    required=True,
    help="Survey name among ztf or lsst. Note that each survey will have its own configuration file.",
)
@click.option(
    "-username",
    type=str,
    required=True,
    help="username used for the authentication on the Kafka cluster",
)
@click.option(
    "-group_id",
    type=str,
    required=True,
    help="group_id used for the authentication on the Kafka cluster",
)
@click.option(
    "-servers",
    type=str,
    required=True,
    help="Fink Kafka bootstrap server in the form name:port",
)
#@click.option_panel("Main", options=["-survey", "-username"])
@click.command(
    context_settings=CONTEXT_SETTINGS,
    epilog="More information at ...",
)
def register():
    """
    My amazing tool does all the things.

    This is a minimal example based on documentation
    from the 'click' package.

    You can try using --help at the top level and also for
    specific subcommands.
    """
    click.echo("register")

cli.add_command(register)
