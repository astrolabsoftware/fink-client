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
import time
import logging

from astropy.time import Time
from tabulate import tabulate

from fink_client.consumer import extract_id_from_lsst
from fink_client.avro_utils import write_alert

from fink_client.botlib import (
    get_curve_ztf,
    get_curve_lsst,
    get_cutout,
    msg_handler_tg,
    msg_handler_slack,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
_LOG = logging.getLogger(__name__)


def display_alerts_as_table(survey, topic, alert) -> None:
    """Display table based on input survey

    Parameters
    ----------
    survey: str
        lsst or ztf
    topic: str
        Topic name
    alert: dict
        Dictionary containing alert data

    """
    utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    if survey == "ztf":
        table = [
            [
                Time(alert["candidate"]["jd"], format="jd").iso,
                utc,
                topic,
                alert["objectId"],
                alert["cdsxmatch"],
                alert["candidate"]["magpsf"],
            ],
        ]
        header = [
            "Emitted at (UTC)",
            "Received at (UTC)",
            "Topic",
            "objectId",
            "Simbad",
            "Magnitude",
        ]
    elif survey == "lsst":
        id_value, id_name = extract_id_from_lsst(alert)
        table = [
            [
                Time(
                    alert["diaSource"]["midpointMjdTai"], format="mjd", scale="tai"
                ).utc.iso,
                utc,
                topic,
                id_value,
            ]
        ]
        header = ["Emitted at (UTC)", "Received at (UTC)", "Topic", id_name]

    print(tabulate(table, header, tablefmt="pretty"))


def store_alert(
    alert: dict, schema: str, outdir: str, survey: str, overwrite: bool = True
) -> None:
    """Store alerts on disk

    Parameters
    ----------
    alerts: dict
        Dictionary with alert data
    schema: str or dict
        Path to Avro schema of the alert, or parsed schema.
    outdir: str
        Directory where to store alerts
    survey: str
        Survey name, among ztf or lsst
    overwrite: bool, optional
        If True, overwrite files with the same name.
        Default is True.
    """
    if survey == "ztf":
        id1 = "objectId"
        id2 = "candid"
    elif survey == "lsst":
        id1 = "diaSourceId"
        id2 = None

    write_alert(
        alert,
        schema,
        outdir,
        overwrite=overwrite,
        id1=id1,
        id2=id2,
    )


def send_to_telegram(
    alert: dict, survey: str, topic: str, token: str, channel: str
) -> None:
    """Send alerts to a Telegram channel

    Parameters
    ----------
    alerts: dict
        Dictionary with alert data
    survey: str
        Survey name, among ztf or lsst
    topic: str
        Topic name
    token: str
        Telegram bot token
    channel: str
        Name of the Telegram channel. Must start with @.

    """
    if survey == "ztf":
        oid = alert["objectId"]
        curve_png, status_code_curve = get_curve_ztf(alert, origin="alert")
        cutout, status_code_cutout = get_cutout(
            cutout=alert["cutoutScience"]["stampData"], gzipped=True
        )

    elif survey == "lsst":
        oid = alert["diaSource"]["diaObjectId"]
        curve_png, status_code_curve = get_curve_lsst(
            alert,
            origin="alert",
        )

        cutout, status_code_cutout = get_cutout(
            cutout=alert["cutoutScience"], gzipped=False
        )

    text = f"""
*Object ID*: [{oid}](https://{survey}.fink-portal.org/{oid})
*Topic*: `{topic}`
    """

    if status_code_curve != 200:
        _LOG.warning(
            "Error {} when downloading lightcurve for object {}".format(
                status_code_curve, oid
            )
        )
    if status_code_cutout != 200:
        _LOG.warning(
            "Error {} when downloading cutout for object {}".format(
                status_code_cutout, oid
            )
        )

    msg_handler_tg(
        [(text, curve_png, cutout)],
        channel_id=channel,
        init_msg="",
        token=token,
    )


def send_to_slack(
    alert: dict, survey: str, topic: str, token: str, channel: str
) -> None:
    """Send alerts to a Slack channel

    Parameters
    ----------
    alerts: dict
        Dictionary with alert data
    survey: str
        Survey name, among ztf or lsst
    topic: str
        Topic name
    token: str
        Slack bot token
    channel: str
        Name of the Slack channel. Must start with @.

    """
    if survey == "ztf":
        oid = alert["objectId"]
        curve_png, status_code_curve = get_curve_ztf(alert, origin="alert")
        cutout, status_code_cutout = get_cutout(
            cutout=alert["cutoutScience"]["stampData"], gzipped=True
        )

    elif survey == "lsst":
        oid = alert["diaSource"]["diaObjectId"]
        curve_png, status_code_curve = get_curve_lsst(
            alert,
            origin="alert",
        )

        cutout, status_code_cutout = get_cutout(
            cutout=alert["cutoutScience"], gzipped=False
        )

    text = f"""
*Object ID*: https://{survey}.fink-portal.org/{oid}
*Topic*: `{topic}`
    """

    if status_code_curve != 200:
        _LOG.warning(
            "Error {} when downloading lightcurve for object {}".format(
                status_code_curve, oid
            )
        )
    if status_code_cutout != 200:
        _LOG.warning(
            "Error {} when downloading cutout for object {}".format(
                status_code_cutout, oid
            )
        )

    msg_handler_slack(
        text,
        curve_png,
        cutout,
        channel_id=channel,
        init_msg="",
        token=token,
    )
