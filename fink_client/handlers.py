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

from fink_client.consumer import extract_id_from_lsst
from fink_client.avro_utils import write_alert


def display_alerts_as_table(survey, topic, alert, is_mma=False):
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

    Returns
    -------
    table: list of list
    header: list of str
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
    return table, header


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
