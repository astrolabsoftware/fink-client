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
from fink_client.configuration import write_credentials
from fink_client.configuration import load_credentials
from fink_client.logger import get_fink_logger


_LOG = get_fink_logger()


def register_(survey, username, group_id, servers, log_level, maxtimeout, tmp):
    _LOG.setLevel(log_level)

    config = {
        "survey": survey,
        "username": username,
        "groupid": group_id,
        "servers": servers,
        "maxtimeout": maxtimeout,
    }

    # Write credentials
    write_credentials(config, log_level, tmp)

    # check credentials are correct
    _LOG.debug("Credentials are: {}".format(load_credentials(survey=survey, tmp=tmp)))
