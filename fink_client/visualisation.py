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
import io
import gzip

import matplotlib.pyplot as plt

from astropy.io import fits

import numpy as np

from fink_client.tester import regular_unit_tests


def plot_cutout(stamp: bytes, fig=None, subplot=None, **kwargs):
    """Plot one cutout contained in an alert (2D array)

    Adapted from ZTF alert tools.

    Parameters
    ----------
    stamp: bytes
        Cutout data as raw binary from the alert
    """
    with gzip.open(io.BytesIO(stamp), "rb") as f:
        with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdul:
            if fig is None:
                fig = plt.figure(figsize=(4, 4))
            if subplot is None:
                subplot = (1, 1, 1)

            ax = fig.add_subplot(*subplot)

            # Update graph data for stamps
            data = np.nan_to_num(hdul[0].data)

            data = data[::-1]

            ax.imshow(data)

    return ax


def show_stamps(alert: dict, fig=None):
    """Plot the 3 cutouts contained in an alert.

    Parameters
    ----------
    alert: dict
        Dictionnary containing alert data.
    """
    for i, cutout in enumerate(["Science", "Template", "Difference"]):
        stamp = alert["cutout{}".format(cutout)]["stampData"]
        ffig = plot_cutout(stamp, fig=fig, subplot=(1, 3, i + 1))
        ffig.set_title(cutout)
        # remove axis labels
        plt.xlabel("")
        plt.ylabel("")


def extract_history(history_list: list, field: str) -> list:
    """Extract the historical measurements contained in the alerts for the parameter `field`.

    Parameters
    ----------
    history_list: list of dict
        List of dictionary from alert['prv_candidates'].
    field: str
        The field name for which you want to extract the data. It must be
        a key of elements of history_list (alert['prv_candidates'])

    Returns
    -------
    measurement: list
        List of all the `field` measurements contained in the alerts.
    """
    try:
        measurement = [obs[field] for obs in history_list]
    except KeyError:
        print("{} not in history data".format(field))
        measurement = [None] * len(history_list)

    return measurement


def extract_field(alert: dict, field: str) -> np.array:
    """Concatenate current and historical observation data for a given field.

    Parameters
    ----------
    alert: dict
        Dictionnary containing alert data
    field: str
        Name of the field to extract.

    Returns
    -------
    data: np.array
        List containing previous measurements and current measurement at the
        end. If `field` is not in `prv_candidates fields, data will be
        [None, None, ..., alert['candidate'][field]].

    Examples
    --------
    >>> from fink_client.visualisation import extract_field
    >>> alert = {"candidate": {"magpsf": 1.0}, "prv_candidates": np.array([{"magpsf": 2.0}])}
    >>> mag = extract_field(alert, "magpsf")
    >>> assert len(mag) == 2, mag

    >>> alert = {"candidate": {"magpsf": 1.0}, "prv_candidates": None}
    >>> mag = extract_field(alert, "magpsf")
    >>> assert len(mag) == 1, mag

    >>> alert = {"candidate": {"magpsf": 1.0}, "prv_candidates": [{"magpsf": 2.0}, {"magpsf": None}]}
    >>> mag = extract_field(alert, "magpsf")
    >>> assert len(mag) == 3, mag
    """
    if alert["prv_candidates"] is None:
        data = [alert["candidate"][field]]
    else:
        data = np.concatenate([
            [alert["candidate"][field]],
            extract_history(alert["prv_candidates"], field),
        ])
    return data


if __name__ == "__main__":
    """ Run the test suite """

    regular_unit_tests()
