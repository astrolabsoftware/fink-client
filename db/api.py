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
import time
import pathlib
import sqlite3
import pandas as pd

ALERT_TABLE = "alert-monitoring"

def update_alert_monitoring_db(db_path: str, df: pd.DataFrame):
    """ Insert new DataFrame rows in the database

    Parameters
    ----------
    db_path: str
        Path to the monitoring database. The database will be created if
        it does not exist yet.
    df: pd.DataFrame
        Pandas DataFrame with N rows and with 4 columns:
            - Reception time (float) from time.time() (alert reception time)
            - objectId (str) from alert
            - topic (str) from alert
            - timestamp (str) from alert (alert emission time)

    Examples
    ----------
    >>> df = pd.DataFrame({
    ...     "time": [time.time()],
    ...     "objectId": ["tutu"],
    ...     "topic": ["titi"],
    ...     "timestamp": ['2019-11-21 08:47:37.411'})

    >>> update_alert_monitoring_db(db_fn, df)
    """
    con = sqlite3.connect(db_path)
    df.to_sql(ALERT_TABLE, con, schema=None, if_exists='append')

def get_alert_monitoring_data(
        db_path: str, start: float, end: float) -> pd.DataFrame:
    """ Query alert data monitoring rows between two dates

    Parameters
    ----------
    db_path: str
        Path to the monitoring database. The database will be created if
        it does not exist yet.
    start: float
        Starting date in seconds, excluded (units of time.time())
    end: float
        End date in seconds, included (units of time.time()).

    Returns
    ----------
    df: pd.DataFrame
        Pandas DataFrame with data of matching alert rows.

    Examples
    ----------
    >>> df = get_alert_monitoring_data(db_fn, 0.0, time.time())
    >>> print(len(df[df["objectId"] == "toto"]))
    1
    """

    con = sqlite3.connect(db_path)
    statement = f"""
    SELECT
        time, objectId, topic, timestamp
    FROM
        `{ALERT_TABLE}`
    WHERE
        time > '{start}' AND time <= '{end}';
    """
    try:
        df = pd.read_sql_query(statement, con)
    except pd.io.sql.DatabaseError as e:
        print(e)
        df = pd.DataFrame()
    return df

def get_alert_per_topic(db_path: str, topic: str):
    """ Query all alert data monitoring rows for a given topic

    Parameters
    ----------
    db_path: str
        Path to the monitoring database. The database will be created if
        it does not exist yet.
    topic: str
        Topic name of a stream

    Returns
    ----------
    df: pd.DataFrame
        Pandas DataFrame with data of matching alert rows.

    Examples
    ----------
    >>> df = get_alert_per_topic(db_fn, "tutu")
    >>> print(len(df))
    1
    """
    con = sqlite3.connect(db_path)
    statement = f"SELECT objectId FROM `{ALERT_TABLE}` WHERE topic = '{topic}';"
    try:
        df = pd.read_sql_query(statement, con)
        alert_id = list(df["objectId"])
    except pd.io.sql.DatabaseError as e:
        print(e)
        alert_id = [""]
    return alert_id


if __name__ == "__main__":
    import doctest
    # Numpy introduced non-backward compatible change from v1.14.
    import numpy as np
    if np.__version__ >= "1.14.0":
        np.set_printoptions(legacy="1.13")

    # Initialise test DB
    db_fn = "db_test.db"
    df = pd.DataFrame({
        "time": [time.time()],
        "objectId": ["toto"],
        "topic": ["tutu"],
        "timestamp": ['2019-11-21 08:47:37.411']
    })
    update_alert_monitoring_db(db_fn, df)

    # Run the tests
    global_args = globals()
    global_args["db_fn"] = db_fn
    (failure_count, test_count) = doctest.testmod(globs=global_args)

    # Remove test db
    os.remove(global_args["db_fn"])

    # Exit with test code
    sys.exit(failure_count)
