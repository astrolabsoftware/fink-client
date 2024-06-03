#!/usr/bin/env python
# Copyright 2019-2024 AstroLab Software
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
"""Display cutouts and lightcurve from a ZTF alert"""

import argparse

import matplotlib
import matplotlib.pyplot as plt

import numpy as np

from fink_client.avro_utils import AlertReader
from fink_client.visualisation import show_stamps
from fink_client.visualisation import extract_field

# For plots
font = {"weight": "bold", "size": 22}

matplotlib.rc("font", **font)

# Bands
filter_color = {1: "#1f77b4", 2: "#ff7f0e", 3: "#2ca02c"}
# [
#     '#1f77b4',  # muted blue
#     '#ff7f0e',  # safety orange
#     '#2ca02c',  # cooked asparagus green
#     '#d62728',  # brick red
#     '#9467bd',  # muted purple
#     '#8c564b',  # chestnut brown
#     '#e377c2',  # raspberry yogurt pink
#     '#7f7f7f',  # middle gray
#     '#bcbd22',  # curry yellow-green
#     '#17becf'   # blue-teal
# ]
filter_name = {1: "g band", 2: "r band", 3: "i band"}


def main():
    """ """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-filename",
        type=str,
        default="",
        help="Path to an alert data file (avro format)",
    )
    args = parser.parse_args(None)

    r = AlertReader(args.filename)

    # Display the cutouts contained in the alert
    alert = r.to_list(size=1)[0]
    print(alert["objectId"])
    fig = plt.figure(num=0, figsize=(12, 4))
    show_stamps(alert, fig)

    # extract current and historical data as one vector
    mag = extract_field(alert, "magpsf")
    error = extract_field(alert, "sigmapsf")
    upper = extract_field(alert, "diffmaglim")

    # filter bands
    fid = extract_field(alert, "fid")

    # Rescale dates to end at 0
    jd = extract_field(alert, "jd")
    dates = np.array([i - jd[0] for i in jd])

    # Title of the plot (alert ID)
    title = alert["objectId"]

    # loop over filters
    fig = plt.figure(num=1, figsize=(12, 4))

    # Loop over each filter
    for filt in filter_color.keys():
        mask = np.where(fid == filt)[0]

        # Skip if no data
        if len(mask) == 0:
            continue

        # y data
        maskNotNone = mag[mask] != None  # noqa: E711
        plt.errorbar(
            dates[mask][maskNotNone],
            mag[mask][maskNotNone],
            yerr=error[mask][maskNotNone],
            color=filter_color[filt],
            marker="o",
            ls="",
            label=filter_name[filt],
            mew=4,
        )
        # Upper limits
        plt.plot(
            dates[mask][~maskNotNone],
            upper[mask][~maskNotNone],
            color=filter_color[filt],
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
    plt.show()


if __name__ == "__main__":
    main()
