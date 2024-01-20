---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Download OffsetsDB

OffsetsDB provides a comprehensive and detailed view of carbon offset credits and projects. You can access the data in various formats or directly through Python using our data package.

## CSV

Download the latest version of OffsetsDB in CSV:

- [Download Credits & Projects](https://carbonplan-offsets-db.s3.us-west-2.amazonaws.com/archive/latest/offsets-db.csv.zip)

## Parquet

Download the latest version of OffsetsDB in [Parquet](https://parquet.apache.org/):

- [Download Credits & Projects](https://carbonplan-offsets-db.s3.us-west-2.amazonaws.com/archive/latest/offsets-db.parquet.zip)

## Raw Registry Data

You can also use parquet to access the full archive of OffsetsDB, representing the full history of the produciton of OffsetsDB:

```python
# TK an example of how we want folks to work with archive data
```

## Accessing Data Through Python

For more dynamic and programmatic access to OffsetsDB, you can use our Python data package. This package allows you to load and interact with the data directly in your Python environment.

### Installation

To get started, install the offsets_db_data package. Ensure you have Python installed on your system, and then run:

```bash
python -m pip install offsets-db-data
```

### Using the Data Catalog

Once installed, you can access the data through an Intake catalog. This catalog provides a high-level interface to the OffsetsDB datasets.

Loading the Catalog

```{code-cell} ipython3
from offsets_db_data.data import catalog

# Display the catalog
print(catalog)
```

#### Available Data

The catalog includes different datasets, like credits and projects. You can list the available datasets using:

```{code-cell} ipython3
# List available datasets in the catalog
print(list(catalog.keys()))

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
