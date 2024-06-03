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
import yaml
import os

from fink_client.tester import regular_unit_tests

_ROOTDIR = os.path.join(os.environ["HOME"], ".finkclient")
_CREDNAME = "credentials.yml"


def write_credentials(dict_file: dict, verbose: bool = False, tmp: bool = False):
    """Store user credentials on the computer.

    To get your credentials, contact Fink admins or fill the registration form:
        https://forms.gle/2td4jysT4e9pkf889

    Parameters
    ----------
    dict_file: dict
        Dictionnary containing user credentials.
    verbose: bool, optional
        If True, print the credentials location. Default is False.
    tmp: bool, optional
        If True, store the credentials under /tmp. Default is False.

    Examples
    --------
    >>> conf = {
    ...   'username': 'test',
    ...   'password': None,
    ...   'mytopics': ['rrlyr'],
    ...   'servers': 'localhost:9093',
    ...   'group_id': 'test_group',
    ...   'maxtimeout': 10
    ... }
    >>> write_credentials(conf, verbose=False, tmp=True)
    """
    if tmp:
        ROOTDIR = "/tmp"
    else:
        ROOTDIR = _ROOTDIR

    # check there are no missing information
    mandatory_keys = [
        "username",
        "password",
        "group_id",
        "mytopics",
        "servers",
        "maxtimeout",
    ]
    for k in mandatory_keys:
        assert k in dict_file.keys(), "You need to specify {}".format(k)

    # Create the folder if it does not exist
    os.makedirs(ROOTDIR, exist_ok=True)

    # Store data into yml file
    with open(os.path.join(ROOTDIR, _CREDNAME), "w") as f:
        yaml.dump(dict_file, f)

    if verbose:
        print("Credentials stored at {}/{}".format(ROOTDIR, _CREDNAME))


def load_credentials(tmp: bool = False) -> dict:
    """Load fink-client credentials.

    Parameters
    ----------
    tmp: bool, optional
        If True, load the credentials from /tmp. Default is False.

    Returns
    -------
    creds: dict
        Dictionnary containing user credentials.

    Examples
    --------
    >>> conf_in = {
    ...   'username': 'test',
    ...   'password': None,
    ...   'mytopics': ['rrlyr'],
    ...   'servers': 'localhost:9093',
    ...   'group_id': 'test_group',
    ...   'maxtimeout': 10
    ... }
    >>> write_credentials(conf_in, verbose=False, tmp=True)
    >>> conf_out = load_credentials(tmp=True)

    If, however the credentials do not exist yet
    >>> os.remove('/tmp/credentials.yml')
    >>> conf = load_credentials(tmp=True) # doctest: +NORMALIZE_WHITESPACE, +ELLIPSIS
    Traceback (most recent call last):
     ...
    OSError: No credentials found, did you register?
    To get your credentials, and use fink-client you need to:
      1. subscribe to one or more Fink streams at
        https://forms.gle/2td4jysT4e9pkf889
      2. run `fink_client_register` to register
    See https://fink-broker.readthedocs.io/en/latest/services/data_transfer/

    """
    if tmp:
        ROOTDIR = "/tmp"
    else:
        ROOTDIR = _ROOTDIR

    path = os.path.join(ROOTDIR, _CREDNAME)

    if not os.path.exists(path):
        msg = """
        No credentials found, did you register?
        To get your credentials, and use fink-client you need to:
          1. subscribe to one or more Fink streams at
            https://forms.gle/2td4jysT4e9pkf889
          2. run `fink_client_register` to register
        See https://fink-broker.readthedocs.io/en/latest/services/data_transfer/
        """
        raise IOError(msg)

    with open(path) as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    return creds


def mm_topic_names():
    """Return list of topics with MMA schema"""
    out = [
        "fink_grb_bronze",
        "fink_grb_silver",
        "fink_grb_gold",
        "fink_gw_bronze",
    ]
    return out


if __name__ == "__main__":
    """ Run the test suite """

    regular_unit_tests(globals())
