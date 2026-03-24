#!/usr/bin/env python
# Copyright 2023-2026 AstroLab Software
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
"""Utilities to convert avro records to arrow format"""

from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.compute as pc

from astropy.time import Time

from fink_client.tester import regular_unit_tests
from fink_client.logger import get_fink_logger

_LOG = get_fink_logger("Fink", "INFO")


def avro_to_arrow(avro_schema: Dict[str, Any], records: List[Dict[str, Any]]):
    """Convert Avro records to an Arrow table.

    Parameters
    ----------
    avro_schema: dict
        Avro schema as dictionary
    records: list of dicts
        List of Avro records

    Returns
    -------
    out: PyArrow Table
    out: PyArrow Schema

    Examples
    --------
    Convert ZTF Avro records
    >>> from fink_client.avro_utils import _get_alert_schema, AlertReader
    >>> records = AlertReader(avro_ztf).to_list()
    >>> avro_schema = _get_alert_schema(schema_ztf)
    >>> table, arrow_schema = avro_to_arrow(avro_schema, records)

    Convert LSST Avro records
    >>> from fink_client.avro_utils import _get_alert_schema, AlertReader
    >>> records = AlertReader(avro_lsst).to_list()
    >>> avro_schema = _get_alert_schema(schema_lsst)
    >>> table, arrow_schema = avro_to_arrow(avro_schema, records)
    """
    # Step 1: Convert Avro schema to Arrow schema
    arrow_schema = avro_schema_to_arrow_schema(avro_schema)

    # Step 2: Convert records to Arrow-compatible format
    arrow_data = transform_records(avro_schema, records)

    # Step 3: Create and return Arrow table
    return pa.table(arrow_data, schema=arrow_schema), arrow_schema


def avro_schema_to_arrow_schema(avro_schema: Dict[str, Any]) -> pa.Schema:
    """Convert Avro schema to PyArrow schema.

    Parameters
    ----------
    avro_schema: dict
        Avro schema as dictionary

    Returns
    -------
    out: pa.Schema
        PyArrow schema

    Examples
    --------
    Convert ZTF Avro schema
    >>> from fink_client.avro_utils import _get_alert_schema
    >>> avro_schema = _get_alert_schema(schema_ztf)
    >>> arrow_schema = avro_schema_to_arrow_schema(avro_schema)

    Convert LSST Avro records
    >>> from fink_client.avro_utils import _get_alert_schema
    >>> avro_schema = _get_alert_schema(schema_lsst)
    >>> arrow_schema = avro_schema_to_arrow_schema(avro_schema)
    """
    if avro_schema.get("type") == "record":
        fields = []
        for field in avro_schema.get("fields", []):
            field_name = field["name"]
            field_type = avro_type_to_arrow_type(field["type"])
            fields.append(pa.field(field_name, field_type))
        return pa.schema(fields)
    else:
        raise ValueError("Root schema must be a record type")


def avro_type_to_arrow_type(avro_type: Any) -> pa.DataType:
    """Convert a single Avro type to PyArrow type.

    Notes
    -----
    Handles: primitives, records, arrays, maps, unions with null.

    Parameters
    ----------
    avro_type: Any
        Any single Avro type

    Returns
    -------
    out: pa.DataType
        Corresponding PyArrow DataType
    """
    # Handle union types (e.g., ["long", "null"])
    if isinstance(avro_type, list):
        # Filter out "null" to get the actual type
        non_null_types = [t for t in avro_type if t != "null"]

        if len(non_null_types) == 0:
            return pa.null()
        elif len(non_null_types) == 1:
            # Nullable type: convert the non-null type
            return avro_type_to_arrow_type(non_null_types[0])
        else:
            # Multiple non-null types (union) - use dense union
            # For simplicity, we'll use the first type
            return avro_type_to_arrow_type(non_null_types[0])

    # Handle string type names (primitives)
    if isinstance(avro_type, str):
        type_map = {
            "null": pa.null(),
            "boolean": pa.bool_(),
            "int": pa.int32(),
            "long": pa.int64(),
            "float": pa.float32(),
            "double": pa.float64(),
            "bytes": pa.binary(),
            "string": pa.string(),
        }
        if avro_type in type_map:
            return type_map[avro_type]
        raise ValueError(f"Unknown Avro type: {avro_type}")

    # Handle complex types (dict)
    if isinstance(avro_type, dict):
        avro_type_name = avro_type.get("type")

        # Record type
        if avro_type_name == "record":
            fields = []
            for field in avro_type.get("fields", []):
                field_name = field["name"]
                field_type = avro_type_to_arrow_type(field["type"])
                fields.append(pa.field(field_name, field_type))
            return pa.struct(fields)

        # Array type
        elif avro_type_name == "array":
            item_type = avro_type_to_arrow_type(avro_type["items"])
            return pa.list_(item_type)

        # Map type
        elif avro_type_name == "map":
            value_type = avro_type_to_arrow_type(avro_type["values"])
            return pa.map_(pa.string(), value_type)

        else:
            raise ValueError(f"Unknown complex Avro type: {avro_type_name}")

    raise ValueError(f"Cannot convert Avro type: {avro_type}")


def transform_records(
    avro_schema: Dict[str, Any], records: List[Dict[str, Any]]
) -> Dict[str, List[Any]]:
    """Transform Avro records into Arrow-compatible format.

    Notes
    -----
    Handles nested records, arrays, maps, and null values.

    Parameters
    ----------
    avro_schema: dict
        Avro schema as dictionary
    records: list of dicts
        List of Avro records

    Returns
    -------
    out: dict
        Arrow-compatible data
    """
    fields = avro_schema.get("fields", [])
    arrow_data = {field["name"]: [] for field in fields}

    for record in records:
        for field in fields:
            field_name = field["name"]
            field_type = field["type"]
            value = record.get(field_name)

            # Transform the value based on its type
            transformed_value = transform_value(value, field_type)
            arrow_data[field_name].append(transformed_value)

    return arrow_data


def transform_value(value: Any, avro_type: Any) -> Any:
    """Transform a single Avro value to Arrow-compatible format.

    Notes
    -----
    Recursively handles nested structures.

    Parameters
    ----------
    value: Any
        Any value
    avro_type: Any
        Corresponding Avro type

    Returns
    -------
    out: Any
        Corresponding value in Arrow-compatible format
    """
    # Handle None/null
    if value is None:
        return None

    # Handle union types
    if isinstance(avro_type, list):
        non_null_types = [t for t in avro_type if t != "null"]
        if len(non_null_types) > 0:
            return transform_value(value, non_null_types[0])
        return None

    # Handle primitive types (strings)
    if isinstance(avro_type, str):
        return value  # Primitives pass through as-is

    # Handle complex types (dicts)
    if isinstance(avro_type, dict):
        avro_type_name = avro_type.get("type")

        # Record type: convert dict to tuple (for struct)
        if avro_type_name == "record":
            fields = avro_type.get("fields", [])
            transformed_record = {}
            for field in fields:
                field_name = field["name"]
                field_value = value.get(field_name) if isinstance(value, dict) else None
                transformed_record[field_name] = transform_value(
                    field_value, field["type"]
                )
            return transformed_record

        # Array type: transform each item
        elif avro_type_name == "array":
            if not isinstance(value, list):
                return None
            item_type = avro_type["items"]
            return [transform_value(item, item_type) for item in value]

        # Map type: transform each value
        elif avro_type_name == "map":
            if not isinstance(value, dict):
                return None
            value_type = avro_type["values"]
            return {k: transform_value(v, value_type) for k, v in value.items()}

    return value


def create_partitioning(table, arrow_schema, partitionby, survey):
    """
    Add partitioning columns to Arrow table and schema.

    Parameters
    ----------
    table: PyArrow Table
        Table with alert content
    arrow_schema: PyArrow Schema
        Schema of the Table
    partitionby: str
        Partitioning strategy: time, finkclass, tnsclass, or classId
    survey: str
        Survey name: ztf, lsst

    Returns
    -------
    out: tuple
        Updated table, updated schema, partitioning column names
    """
    # partitioning colum if need be
    if survey == "ztf":
        timecol = "jd"
        format_timecol = "jd"
        timesection = "candidate"
    elif survey == "lsst":
        timecol = "midpointMjdTai"
        format_timecol = "mjd"
        timesection = "diaSource"

    time_cols_in_table = [
        col for col in ["year", "month", "day"] if col in table.column_names
    ]
    if partitionby == "time":
        if timecol in table.column_names:
            # Parse time column using astropy
            if "year" in table.column_names:
                _LOG.warning("Partitioning columns already exist. Recreating...")
                table = table.drop(time_cols_in_table)

            table, arrow_schema = _add_date_partitions(table, timecol, format_timecol)
            partitioning = ["year", "month", "day"]

        elif timesection in table.column_names:
            # Extract time from nested struct, then parse
            if "year" in table.column_names:
                _LOG.warning("Partitioning columns already exist. Recreating...")
                table = table.drop(time_cols_in_table)
            table, arrow_schema = _add_date_partitions_from_struct(
                table, timesection, timecol, format_timecol
            )
            partitioning = ["year", "month", "day"]
        else:
            partitioning = None

    elif partitionby == "finkclass":
        if "finkclass" not in table.column_names:
            _LOG.warning("finkclass not found. Applying time partitioning.")

            if timecol in table.column_names:
                if "year" in table.column_names:
                    _LOG.warning("Partitioning columns already exist. Recreating...")
                    table = table.drop(time_cols_in_table)
                table, arrow_schema = _add_date_partitions(
                    table, timecol, format_timecol
                )
                partitioning = ["year", "month", "day"]
            else:
                _LOG.error(f"Neither finkclass nor {timecol} found in table.")
                partitioning = None
        else:
            partitioning = ["finkclass"]

    elif partitionby == "tnsclass":
        partitioning = ["tnsclass"]

    elif partitionby == "classId":
        partitioning = ["classId"]

    else:
        partitioning = None

    return table, arrow_schema, partitioning


def _add_date_partitions(table, timecol, format_timecol):
    """Extract year, month, day from a time column.

    Parameters
    ----------
    table: pyarrow table
        Alert table
    timecol: str
        Name of the column with times
    format_timecol: str
        Time format: mjd, jd

    Returns
    -------
    table: pyarrow table
        Updated alert table
    arrow_schema: dict
        Dictionary for the updated table
    """
    # Get the time column
    time_array = table[timecol]

    # Convert to Python objects for astropy parsing
    time_strings = pc.cast(time_array, pa.string()).to_pylist()

    # Parse with astropy and extract year, month, day
    years = []
    months = []
    days = []

    for time_str in time_strings:
        if time_str is None:
            years.append(None)
            months.append(None)
            days.append(None)
        else:
            try:
                t = Time(time_str, format=format_timecol)
                date_str = t.strftime("%Y-%m-%d")
                y, m, d = date_str.split("-")
                years.append(y)
                months.append(m)
                days.append(d)
            except Exception as e:
                _LOG.warning(f"Failed to parse time '{time_str}': {e}")
                years.append(None)
                months.append(None)
                days.append(None)

    # Add columns to table
    table = (
        table
        .append_column("year", pa.array(years, type=pa.string()))
        .append_column("month", pa.array(months, type=pa.string()))
        .append_column("day", pa.array(days, type=pa.string()))
    )

    # Update schema
    arrow_schema = table.schema

    return table, arrow_schema


def _add_date_partitions_from_struct(table, timesection, timecol, format_timecol):
    """Extract year, month, day from a time field inside a struct column.

    Parameters
    ----------
    table: pyarrow table
        Alert table
    timesection: str
        Name of the struct column containing time column
    timecol: str
        Name of the column with times
    format_timecol: str
        Time format: mjd, jd

    Returns
    -------
    table: pyarrow table
        Updated alert table
    arrow_schema: dict
        Dictionary for the updated table
    """
    # Extract the nested field from struct
    struct_col = table[timesection]
    struct_type = struct_col.type

    # Get the field index for timecol within the struct
    field_index = struct_type.get_field_index(timecol)
    if field_index == -1:
        raise ValueError(f"Field '{timecol}' not found in struct '{timesection}'")

    # Extract the time field
    time_array = pc.struct_field(struct_col, [field_index])

    # Convert to Python objects for astropy parsing
    time_values = time_array.to_pylist()

    # Parse with astropy and extract year, month, day
    years = []
    months = []
    days = []

    for time_val in time_values:
        if time_val is None:
            years.append(None)
            months.append(None)
            days.append(None)
        else:
            try:
                t = Time(time_val, format=format_timecol)
                date_str = t.strftime("%Y-%m-%d")
                y, m, d = date_str.split("-")
                years.append(y)
                months.append(m)
                days.append(d)
            except Exception as e:
                _LOG.warning(f"Failed to parse time '{time_val}': {e}")
                years.append(None)
                months.append(None)
                days.append(None)

    # Add columns to table
    table = (
        table
        .append_column("year", pa.array(years, type=pa.string()))
        .append_column("month", pa.array(months, type=pa.string()))
        .append_column("day", pa.array(days, type=pa.string()))
    )

    # Update schema
    arrow_schema = table.schema

    return table, arrow_schema


if __name__ == "__main__":
    """ Run the test suite """

    args = globals()

    # v11
    args["avro_ztf"] = "datatest/ztf/part-3-305711.avro"
    args["avro_lsst"] = "datatest/lsst/part-2-918301.avro"
    args["schema_ztf"] = "datatest/ztf/avro_schema_ftransfer_ztf_2026-03-23_11487.json"
    args["schema_lsst"] = (
        "datatest/lsst/avro_schema_ftransfer_lsst_2026-03-18_803281.json"
    )

    regular_unit_tests(global_args=args)
