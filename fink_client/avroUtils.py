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
import sys
import os
import glob
import doctest

from fastavro import reader, writer
import pandas as pd

import fastavro

from fink_client.consumer import _get_alert_schema
from fink_client.tester import regular_unit_tests

class AlertReader():
    """ Class to load alert Avro files

    Parameters
    ----------
    path: str
        Path to alert Avro file or folder containing alert Avro files.

    Examples
    ----------
    Load a single Avro alert
    >>> r = AlertReader(avro_file)
    >>> list_of_alerts = r.to_list()
    >>> print(len(list_of_alerts))
    1

    Read a bunch of files
    >>> r = AlertReader(avro_folder)
    >>> df = r.to_pandas()
    >>> assert('objectId' in  df.columns)

    """
    def __init__(self, path: str):
        """ Initialise the AlertReader class """
        self.path = path
        self._load_avro_files()

    def _load_avro_files(self, ext_path: str = None):
        """ Load Avro alert data

        Parameters
        ----------
        ext_path: str, optional
            If not None, load explicitly data under `ext_path`.
            Default is None (self.path is used).
        """
        if ext_path is not None:
            path = ext_path
        else:
            path = self.path

        if os.path.isdir(path):
            self.filenames = glob.glob(os.path.join(path, '*.avro'))
        elif path == '':
            print('WARNING: path to avro files is empty')
            self.filenames = []
        elif fastavro.is_avro(path):
            self.filenames = [path]
        else:
            msg = """
            Data path not understood: {}
            You must give an avro file with
            its extension (.avro), or a folder with avro files.
            """.format(path)
            raise IOError(msg)

    def _read_single_alert(self, name: str = None) -> dict:
        """ Read an avro alert, and return data as dictionary

        Parameters
        ----------
        name: str, optional
            Name of the alert to read (avro format).
            Default is None (self.path is used).

        Returns
        ----------
        alert: dict
            Alert data in a dictionary

        Examples
        ----------
        >>> r = AlertReader("")
        WARNING: path to avro files is empty
        >>> alert = r._read_single_alert(name=avro_file)
        """
        if name is None:
            name = self.path

        with open(name, 'rb') as fo:
            avro_reader = reader(fo)

            # One alert per file only
            return avro_reader.next()

    def to_pandas(self) -> pd.DataFrame:
        """ Read Avro alert(s) and return data as Pandas DataFrame

        Returns
        ----------
        alert: pd.DataFrame
            Alert data in a pandas DataFrame

        Examples
        ----------
        >>> r = AlertReader(avro_folder)
        >>> df = r.to_pandas()
        >>> assert('objectId' in r.to_pandas().columns)

        """
        return pd.DataFrame.from_records(self.to_list())

    def to_list(self, size: int = None) -> list:
        """ Read Avro alert and return data as list of dictionary

        Returns
        ----------
        out: list of dictionary
            Alert data (dictionaries) in a list
        size: int, optional
            If not None, return only `size` alerts.
            Default is None.

        Examples
        ----------
        >>> r = AlertReader(avro_file)
        >>> mylist = r.to_list()
        >>> print(len(mylist))
        1

        >>> r = AlertReader(avro_folder)
        >>> mylist = r.to_list(size=2)
        >>> print(len(mylist))
        2
        """
        return [self._read_single_alert(fn) for fn in self.filenames[:size]]

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
    >>> r = AlertReader(avro_file)
    >>> alert = r.to_list(size=1)[0]

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
    OSError: ./ZTF19acihgng.avro already exists!
    """
    alert_filename = os.path.join(path, "{}.avro".format(alert["objectId"]))

    # Check if the alert already exist
    if os.path.exists(alert_filename) and not overwrite:
        raise IOError("{} already exists!".format(alert_filename))

    with open(alert_filename, 'wb') as out:
        writer(out, _get_alert_schema(schema), [alert])


if __name__ == "__main__":
    """ Run the test suite """

    args = globals()
    args['avro_file'] = 'datatest/ZTF19acihgng.avro'
    args['avro_folder'] = 'datatest'
    args['schema_path'] = 'schemas/distribution_schema_0p2.avsc'

    regular_unit_tests(global_args=args)
