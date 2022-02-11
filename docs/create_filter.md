# Create a filter in Fink

## What is a filter?

A filter is typically a Python routine that selects which alerts need to be sent based on user-defined criteria. Criteria are based on the alert entries: position, flux, properties, ... and added values by the broker. You can find what is in the alert package in these links:

- Original ZTF data: https://zwickytransientfacility.github.io/ztf-avro-alert/schema.html
- Added value from Fink: https://fink-portal.org/api

Filters run directly on our servers, and at the end of the Fink pipelines.

## How to create a filter?

First, identify alert entries of interest for your filter (do I need magnitude? cross-match information? etc.). Then, you have two choices:

1. [Standard] Open an issue in the [fink-filters](https://github.com/astrolabsoftware/fink-filters) repository describing your needs. We will contact you back to discuss possibilities for building a filter.
2. [Expert] Fork and clone the [fink-filters](https://github.com/astrolabsoftware/fink-filters) repo, and add your filter directly. Then open a Pull Request that will be reviewed by our team.

Keep in mind, LSST incoming stream will be about 10 million alerts per night, or ~1TB/night. Hence the filter must focus on a specific aspect of the stream, to reduce the outgoing volume of alerts. Based on your submission, we will also try to provide estimate of the volume to be transferred.

## Concretely, what is a filter?

In this example, let's imagine you want to receive all alerts flagged as `RRLyr` by the xmatch module. You would create a file called filter.py and define a simple routine:

```python
@pandas_udf(BooleanType(), PandasUDFType.SCALAR) # <- mandatory
def rrlyr(cdsxmatch: Any) -> pd.Series:
    """ Return alerts identified as RRLyr by the xmatch module.

    Parameters
    ----------
    cdsxmatch: Spark DataFrame Column
        Alert column containing the cross-match values

    Returns
    ----------
    out: pandas.Series of bool
        Return a Pandas DataFrame with the appropriate flag:
        false for bad alert, and true for good alert.

    """
    # Here goes your logic
    mask = cdsxmatch.values == "RRLyr"

    return pd.Series(mask)
```

Remarks:

* Note the use of the decorator is mandatory. It is a decorator for Apache Spark, and it specifies the output type as well as the type of operation. Just copy and paste it for simplicity.
* The name of the routine will be used in the name of the Kafka topic. Hence choose a meaningful name!
* The name of the input argument must match the name of an alert field (we programmatically match names in the processing). Here `cdsxmatch` is one column added by the `xmatch` module. See above for the available alert fields.
* You can have several input columns. Just add them one after the other:

```python
@pandas_udf(BooleanType(), PandasUDFType.SCALAR) # <- mandatory
def filter_w_several_input(acol: Any, anothercol: Any) -> pd.Series:
    """ Documentation """
    mask = acol.values == ...
    mask *= anothercol.values == ...

    return pd.Series(mask)s
```
