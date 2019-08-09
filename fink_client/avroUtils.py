#!/usr/bin/env python
# Copyright 2019 AstroLab Software
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
import sys
import os

from fastavro import reader, writer

from fink_client.consumer import _get_alert_schema

def read_alert(alert_filename: str) -> dict:
    """ Read avro alert saved by the dashboard

    Parameters
    ----------
    alert_filename: str
        Name of the alert to read (avro format)

    Returns
    ----------
    alert: dict
        Alert data in a dictionary

    Examples
    ----------
    >>> fn = 'tests/data/fink-broker-sample-alert-1.avro'
    >>> alert = read_alert(fn)
    >>> assert(alert["objectId"] == 'ZTF17aabuxzj')
    """
    with open(alert_filename, 'rb') as fo:
        avro_reader = reader(fo)

        # One alert per file only
        return avro_reader.next()

def write_alert(alert: dict, schema: str, path: str, overwrite: bool = False):
    """ Write avro alert on disk

    Parameters
    ----------
    alert: dict
        Alert data to save (dictionary with avro syntax)
    schema: str
        Path to Avro schema of the alert.
    path: str
        Folder that will contain the alert. The filename will always be
        <objectID>.avro

    Examples
    ----------
    >>> fn_in = 'tests/data/fink-broker-sample-alert-1.avro'
    >>> schema_path = 'schemas/fink_alert_schema.avsc'
    >>> alert = read_alert(fn_in)

    Write the alert on disk
    >>> write_alert(alert, schema_path, ".", overwrite=True)

    For test purposes, you can overwrite alert data on disk, but that should
    not happen in production as alert ID must be unique! Hence the writer will
    raise an exception if overwrite is not specified (default).
    >>> write_alert(
    ...     alert, schema_path, ".", overwrite=False)
    ... # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
      ...
    OSError: ./ZTF17aabuxzj.avro already exists!
    """
    alert_filename = os.path.join(path, "{}.avro".format(alert["objectId"]))

    # Check if the alert already exist
    if os.path.exists(alert_filename) and not overwrite:
        raise IOError("{} already exists!".format(alert_filename))

    with open(alert_filename, 'wb') as out:
        writer(out, _get_alert_schema(schema), [alert])


if __name__ == "__main__":
    import doctest
    # Numpy introduced non-backward compatible change from v1.14.
    import numpy as np
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    sys.exit(doctest.testmod()[0])
