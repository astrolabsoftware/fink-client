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

import argparse


_LOG = get_fink_logger()


def main():
    """ """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-survey",
        type=str,
        required=True,
        help="Survey name among ztf or lsst. Note that each survey will have its own configuration file.",
    )
    parser.add_argument(
        "-username",
        type=str,
        required=True,
        help="username used for the authentication on the Kafka cluster",
    )
    parser.add_argument(
        "-group_id",
        type=str,
        required=True,
        help="group_id used for the authentication on the Kafka cluster",
    )
    parser.add_argument(
        "-servers",
        type=str,
        required=True,
        help="Fink Kafka bootstrap server in the form name:port",
    )
    parser.add_argument(
        "-password",
        type=str,
        default=None,
        help="If specified, password for the authentication. Default is None.",
    )
    parser.add_argument(
        "-mytopics",
        nargs="+",
        type=str,
        default=[],
        help="Space-separated list of subscribed topics. E.g. --topics t1 t2",
    )
    parser.add_argument(
        "-maxtimeout",
        type=int,
        default=10,
        help="Timeout when polling the servers. Default is 10 seconds.",
    )
    parser.add_argument(
        "-log_level",
        type=str,
        default="WARN",
        help="Level of verbosity. Default is WARN. Set to INFO or DEBUG to get more information",
    )
    parser.add_argument(
        "--tmp", action="store_true", help="If specified, register credentials in /tmp."
    )
    args = parser.parse_args(None)
    _LOG.setLevel(args.log_level)

    if args.password == "None":
        pwd = None
    else:
        pwd = args.password

    dict_file = {
        "survey": args.survey,
        "username": args.username,
        "password": pwd,
        "group_id": args.group_id,
        "mytopics": args.mytopics,
        "servers": args.servers,
        "maxtimeout": args.maxtimeout,
    }

    # Write credentials
    write_credentials(dict_file, args.log_level, args.tmp)

    # check credentials are correct
    _LOG.debug(
        "Credentials are: {}".format(load_credentials(survey=args.survey, tmp=args.tmp))
    )


if __name__ == "__main__":
    main()
