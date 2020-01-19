#!/usr/bin/env python
# Copyright 2019-2020 AstroLab Software
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
import matplotlib

from astropy.io import fits
import aplpy

import numpy as np

def plot_cutout(
        stamp: bytes, stretch: str = 'arcsinh', vmid: float = 10,
        fig=None, subplot=None, **kwargs) -> aplpy.FITSFigure:
    """ Plot one cutout contained in an alert (2D array)

    Adapted from ZTF alert tools.

    Parameters
    ----------
    stamp: bytes
        Cutout data as raw binary from the alert
    """
    with gzip.open(io.BytesIO(stamp), 'rb') as f:
        with fits.open(io.BytesIO(f.read())) as hdul:
            if fig is None:
                fig = plt.figure(figsize=(4, 4))
            if subplot is None:
                subplot = (1, 1, 1)
            ffig = aplpy.FITSFigure(
                hdul[0], figure=fig, subplot=subplot, **kwargs)
            ffig.show_grayscale(stretch=stretch, vmid=vmid)
    return ffig

def show_stamps(alert: dict, fig=None):
    """ Plot the 3 cutouts contained in an alert.

    Parameters
    ----------
    alert: dict
        Dictionnary containing alert data.
    """
    for i, cutout in enumerate(['Science', 'Template', 'Difference']):
        stamp = alert['cutout{}'.format(cutout)]['stampData']
        ffig = plot_cutout(stamp, fig=fig, subplot=(1, 3, i + 1))
        ffig.set_title(cutout)
        # remove axis labels
        plt.xlabel('')
        plt.ylabel('')

def extract_history(history_list: list, field: str) -> list:
    """Extract the historical measurements contained in the alerts
    for the parameter `field`.

    Parameters
    ----------
    history_list: list of dict
        List of dictionary from alert['prv_candidates'].
    field: str
        The field name for which you want to extract the data. It must be
        a key of elements of history_list (alert['prv_candidates'])

    Returns
    ----------
    measurement: list
        List of all the `field` measurements contained in the alerts.
    """
    try:
        measurement = [obs[field] for obs in history_list]
    except KeyError as e:
        print('{} not in history data')
        measurement = [None] * len(history_list)

    return measurement

def extract_field(alert: dict, field: str) -> np.array:
    """ Concatenate current and historical observation data for a given field.

    Parameters
    ----------
    alert: dict
        Dictionnary containing alert data
    field: str
        Name of the field to extract.

    Returns
    ----------
    data: np.array
        List containing previous measurements and current measurement at the
        end. If `field` is not in `prv_candidates fields, data will be
        [None, None, ..., alert['candidate'][field]].
    """
    data = np.concatenate(
        [
            [alert["candidate"][field]],
            extract_history(alert['prv_candidates'], field)
        ]
    )
    return data
