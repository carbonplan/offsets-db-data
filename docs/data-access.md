---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Access OffsetsDB Data

OffsetsDB provides a detailed view of carbon offset credits and projects.
You can access the data in various formats or directly through Python using our data package.

```{important}
By downloading or accessing the OffsetsDB data archives, you agree to the [Terms of Data Access](TERMS-OF-DATA-ACCESS.md).
```

## CSV & Parquet Zipped Files

Download the latest version of OffsetsDB in CSV:

- [Download Credits & Projects](https://carbonplan-offsets-db.s3.us-west-2.amazonaws.com/production/latest/offsets-db.csv.zip)

Download the latest version of OffsetsDB in [Parquet](https://parquet.apache.org/):

- [Download Credits & Projects](https://carbonplan-offsets-db.s3.us-west-2.amazonaws.com/production/latest/offsets-db.parquet.zip)

## Citation

Please cite OffsetsDB as:

CarbonPlan (2024) “OffsetsDB” [https://carbonplan.org/research/offsets-db](https://carbonplan.org/research/offsets-db)

## Accessing The Full Data Archive Through Python

For more dynamic and programmatic access to OffsetsDB, you can use our Python data package. This package allows you to load and interact with the data directly in your Python environment. With the data package, you can access the data in a variety of formats including CSV (for raw data) and Parquet (for processed data).

### Installation

To get started, install the offsets_db_data package. Ensure you have Python installed on your system, and then run:

```bash
python -m pip install offsets-db-data
```

### Using the Data Catalog

Once installed, you can access the data through an Intake catalog. This catalog provides a high-level interface to the OffsetsDB datasets.

Loading the Catalog

```{code-cell} ipython3
import pandas as pd
pd.options.display.max_columns = 5
from offsets_db_data.data import catalog

# Display the catalog
print(catalog)
```

#### Available Data

The catalog includes different datasets, like credits and projects

#### Getting Descriptive Information About a Dataset

You can get information about a dataset using the `describe()` method. For example, to get information about the 'credits' dataset:

```{code-cell} ipython3
catalog['credits'].describe()
```

#### Accessing Specific Datasets

You can access individual datasets within the catalog. For example, to access the 'credits' dataset:

```{code-cell} ipython3
# Access the 'credits' dataset
credits = catalog['credits']

# Read the data into a pandas DataFrame
credits_df = credits.read()
credits_df.head()

```

Similarly, to access the 'projects' dataset:

```{code-cell} ipython3
# Access the 'projects' dataset
projects = catalog['projects']

# Read the data into a pandas DataFrame
projects_df = projects.read()
projects_df.head()
```

Calling `projects.read()` and `credits.read()` without specifying a date, will return the data downloaded and processed on `2024-02-13`.

To load data for a specific date, you can specify the date as a string in the format `YYYY-MM-DD`. For example:

```{code-cell} ipython3
projects_df = catalog['projects'](date='2024-02-07').read()
projects_df.head()
```

```{note}
If you specify a date for which the data is not available, the package will raise a `PermissionError: Access Denied`.
```
