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
from fink_client.configuration import write_credentials
from fink_client.configuration import load_credentials

import argparse


def main():
    """ """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-username",
        type=str,
        default="",
        help="username used for the authentication on the Kafka cluster",
    )
    parser.add_argument(
        "-password",
        type=str,
        default=None,
        help="If specified, password for the authentication. Default is None.",
    )
    parser.add_argument(
        "-group_id",
        type=str,
        default="",
        help="group_id used for the authentication on the Kafka cluster",
    )
    parser.add_argument(
        "-mytopics",
        nargs="+",
        type=str,
        default=[],
        help="Space-separated list of subscribed topics. E.g. --topics t1 t2",
    )
    parser.add_argument(
        "-servers",
        type=str,
        default=".",
        help="Comma-separated list of IP:PORT as a single string. e.g. 's1,s2'",
    )
    parser.add_argument(
        "-maxtimeout",
        type=int,
        default=10,
        help="Timeout when polling the servers. Default is 10 seconds.",
    )
    parser.add_argument(
        "--tmp", action="store_true", help="If specified, register credentials in /tmp."
    )
    parser.add_argument(
        "--verbose", action="store_true", help="If specified, print useful information."
    )
    args = parser.parse_args(None)

    if args.password == "None":
        pwd = None
    else:
        pwd = args.password

    dict_file = {
        "username": args.username,
        "password": pwd,
        "group_id": args.group_id,
        "mytopics": args.mytopics,
        "servers": args.servers,
        "maxtimeout": args.maxtimeout,
    }

    # Write credentials
    write_credentials(dict_file, args.verbose, args.tmp)

    # check credentials are correct
    if args.verbose:
        print(load_credentials(args.tmp))


if __name__ == "__main__":
    main()
