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
"""Display cutouts and lightcurve from an alert"""

import argparse
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

from fink_client.consumer import extract_id_from_lsst
from fink_client.avro_utils import AlertReader
from fink_client.visualisation import show_stamps, extract_field
from fink_client.configuration import check_survey_exists

# For plots
font = {"weight": "bold", "size": 22}

matplotlib.rc("font", **font)

BANDS = ["u", "g", "r", "i", "z", "y"]
BANDS_DICT = {k: i + 1 for i, k in enumerate(BANDS)}

DEFAULT_FINK_COLORS = ["#15284f", "#626d84", "#afb2b9", "#dbbeb2", "#e89070", "#f5622e"]

DEFAULT_FINK_MARKERS = {
    "u": "o",  # Matplotlib 'o' -> Plotly 'circle'
    "g": "<",
    "r": ">",
    "i": "s",  # Matplotlib 's' -> Plotly 'square'
    "z": "*",  # Matplotlib '*' -> Plotly 'star'
    "y": "p",  # Matplotlib 'p' -> Plotly 'pentagon'
}


def main():
    """ """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-f",
        type=str,
        required=True,
        help="Path to an alert data file (avro format)",
    )
    parser.add_argument(
        "-survey",
        type=str,
        required=True,
        help="Survey name among ztf or lsst.",
    )
    args = parser.parse_args(None)

    check_survey_exists(args.survey)

    r = AlertReader(args.f)

    # Display the cutouts contained in the alert
    alert = r.to_list(size=1)[0]
    fig = plt.figure(num=0, figsize=(12, 4))
    show_stamps(alert, args.survey, fig)

    if args.survey == "ztf":
        # extract current and historical data as one vector
        mag = extract_field(
            alert, "magpsf", current="candidate", previous="prv_candidates"
        )
        error = extract_field(
            alert, "sigmapsf", current="candidate", previous="prv_candidates"
        )
        upper = extract_field(
            alert, "diffmaglim", current="candidate", previous="prv_candidates"
        )

        # filter bands
        fid = extract_field(
            alert, "fid", current="candidate", previous="prv_candidates"
        )

        # Rescale dates to end at 0
        jd = extract_field(alert, "jd", current="candidate", previous="prv_candidates")
        dates = np.array([i - jd[0] for i in jd])

        # Title of the plot (alert ID)
        title = alert["objectId"]

        # loop over filters
        fig = plt.figure(num=1, figsize=(12, 8))

        # Loop over each filter
        for band, index in BANDS_DICT.items():
            mask = np.where(fid == index)[0]

            # Skip if no data
            if len(mask) == 0:
                continue

            # y data
            maskNotNone = mag[mask] != None  # noqa: E711
            plt.errorbar(
                dates[mask][maskNotNone],
                mag[mask][maskNotNone],
                yerr=error[mask][maskNotNone],
                color=DEFAULT_FINK_COLORS[index - 1],
                marker=DEFAULT_FINK_MARKERS[band],
                ls="",
                label=band,
                mew=4,
            )
            # Upper limits
            plt.plot(
                dates[mask][~maskNotNone],
                upper[mask][~maskNotNone],
                color=DEFAULT_FINK_COLORS[index - 1],
                marker="v",
                ls="",
                mew=4,
                alpha=0.5,
            )
            plt.title(title)
        plt.legend()
        plt.gca().invert_yaxis()
        plt.xlabel("Days to candidate")
        plt.ylabel("Difference magnitude")
    elif args.survey == "lsst":
        # extract current and historical data as one vector
        flux = extract_field(
            alert, "psfFlux", current="diaSource", previous="prvDiaSources"
        )
        error = extract_field(
            alert, "psfFluxErr", current="diaSource", previous="prvDiaSources"
        )

        # filter bands
        fid = extract_field(
            alert, "band", current="diaSource", previous="prvDiaSources"
        )

        # Rescale dates to end at 0
        mjd = extract_field(
            alert, "midpointMjdTai", current="diaSource", previous="prvDiaSources"
        )
        dates = np.array([i - mjd[0] for i in mjd])

        # Title of the plot (alert ID)
        title, _ = extract_id_from_lsst(alert)

        # loop over filters
        fig = plt.figure(num=1, figsize=(12, 8))

        # Loop over each filter
        for band, index in BANDS_DICT.items():
            mask = np.where(np.array(fid) == band)[0]
            # Skip if no data
            if len(mask) == 0:
                continue

            # y data
            plt.errorbar(
                dates[mask],
                flux[mask],
                yerr=error[mask],
                color=DEFAULT_FINK_COLORS[index - 1],
                marker=DEFAULT_FINK_MARKERS[band],
                ls="",
                label=band,
                mew=4,
            )
            plt.title(title)
        plt.legend()
        # plt.gca().invert_yaxis()
        plt.xlabel("Days to candidate")
        plt.ylabel("Difference magnitude")
    plt.show()


if __name__ == "__main__":
    main()
