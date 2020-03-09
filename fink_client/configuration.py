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

ROOTDIR = os.path.join(os.environ['HOME'], ".finkclient")
CREDNAME = "credentials.yml"

def write_credentials(dict_file: dict, verbose: bool = False) -> None:
    """ Store user credentials on the computer.

    To get your credentials, contact Fink admins or fill the registration form:
        https://forms.gle/2td4jysT4e9pkf889

    Parameters
    ----------
    dict_file: dict
        Dictionnary containing user credentials.
    verbose: bool, optional
        If True, print the credentials location.
    """
    # check there are no missing information
    mandatory_keys = [
        'username', 'password', 'group_id',
        'mytopics', 'servers',
        'maxtimeout'
    ]
    for k in mandatory_keys:
        assert k in dict_file.keys(), 'You need to specify {}'.format(k)

    # Create the folder if it does not exist
    os.makedirs(ROOTDIR, exist_ok=True)

    # Store data into yml file
    with open(os.path.join(ROOTDIR, CREDNAME), 'w') as f:
        yaml.dump(dict_file, f)

    if verbose:
        print('Credentials stored at {}/{}'.format(ROOTDIR, CREDNAME))

def load_credentials() -> dict:
    """ Load fink-client credentials.

    Returns
    --------
    creds: dict
        Dictionnary containing user credentials.
    """
    path = os.path.join(ROOTDIR, CREDNAME)

    if not os.path.exists(path):
        msg = """
        No credentials found, did you register?
        To get your credentials, and use fink-client you need to:
          1. subscribe to one or more Fink streams at
            https://forms.gle/2td4jysT4e9pkf889
          2. run `fink_client_registration` to register
        """
        print(msg)

    with open(path) as f:
        creds = yaml.load(f, Loader=yaml.FullLoader)

    return creds
