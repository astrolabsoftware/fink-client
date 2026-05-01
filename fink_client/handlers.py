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

from astropy.time import Time
from tabulate import tabulate

from fink_client.consumer import extract_id_from_lsst
from fink_client.avro_utils import write_alert
from fink_client.visualisation import extract_field

from fink_client.botlib import get_curve, get_cutout, msg_handler_tg


def display_alerts_as_table(survey, topic, alert, is_mma=False) -> None:
    """Display table based on input survey

    Parameters
    ----------
    survey: str
        lsst or ztf
    topic: str
        Topic name
    alert: dict
        Dictionary containing alert data
    is_mma: bool, optional
        If True, assumes MMA topics (ZTF only).

    """
    utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    if survey == "ztf":
        if is_mma:
            table = [
                [
                    alert["objectId"],
                    alert["fink_class"],
                    topic,
                    alert["rate"],
                    alert["observatory"],
                    alert["triggerId"],
                ]
            ]
            header = [
                "ObjectId",
                "Classification",
                "Topic",
                "Rate (mag/day)",
                "Observatory",
                "Trigger ID",
            ]
        else:
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


def store_alert(alert, schema, outdir, survey, is_mma, overwrite=True):
    """TBD"""
    if is_mma:
        id1 = "objectId"
        id2 = "triggerId"
    elif survey == "ztf":
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


def send_to_telegram(alert, survey, token):
    """ """
    if survey == "ztf":
        curve_png = get_curve(
            jd=extract_field(
                alert, "jd", current="candidate", previous="prv_candidates"
            ),
            magpsf=extract_field(
                alert, "magpsf", current="candidate", previous="prv_candidates"
            ),
            sigmapsf=extract_field(
                alert, "sigmapsf", current="candidate", previous="prv_candidates"
            ),
            diffmaglim=extract_field(
                alert, "diffmaglim", current="candidate", previous="prv_candidates"
            ),
            fid=extract_field(
                alert, "fid", current="candidate", previous="prv_candidates"
            ),
            objectId=alert["objectId"],
            origin="fields",
        )

        cutout = get_cutout(cutout=alert["cutoutScience"]["stampData"])

        text = f"""
    *Object ID*: [{alert["objectId"]}](https://ztf.fink-portal.org/{alert["objectId"]})
        """
    elif survey == "lsst":
        mjds = extract_field(
            alert, "midpointMjdTai", current="diaSource", previous="prvDiaSources"
        )
        curve_png = get_curve(
            jd=mjds,
            magpsf=extract_field(
                alert, "scienceFlux", current="diaSource", previous="prvDiaSources"
            ),
            sigmapsf=extract_field(
                alert, "scienceFluxErr", current="diaSource", previous="prvDiaSources"
            ),
            diffmaglim=[None] * len(mjds),
            fid=extract_field(
                alert, "band", current="diaSource", previous="prvDiaSources"
            ),
            objectId=alert["diaSource"]["diaObjectId"],
            origin="fields",
            invert_yaxis=False,
            ylabel="Science Flux [nJy]",
        )

        cutout = get_cutout(cutout=alert["cutoutScience"])

        text = f"""
    *Object ID*: [{alert["diaSource"]["diaObjectId"]}](https://lsst.fink-portal.org/{alert["diaSource"]["diaObjectId"]})
        """

    msg_handler_tg(
        [(text, curve_png, cutout)],
        channel_id="@fink_client_bot_test",
        init_msg="",
        token=token,
    )
