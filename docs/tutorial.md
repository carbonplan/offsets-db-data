---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Getting Started with offsetsDB Data

In this tutorial, we'll be taking a closer look at `offsets-db-data` package, a Python package for loading and working with offsetsDB data. This tutorial assumes that you are already familiar with Python and have some basic knowledge of data manipulation with pandas. You'll need to have Python 3.9 or above installed to use `offsets-db-data` package.

```{note}
If you haven't installed the package yet, please follow the instructions in the [installation guide](install-offsets-db-data.md).
```

First things first, let's import the necessary modules for this tutorial. This guide will walk through the simple steps to get started with an example.

## Loading the offsetsDB data catalog

We'll start by importing the necessary module from the package.

```{code-cell} ipython3
import pandas as pd
pd.options.display.max_columns = 5
from offsets_db_data.data import catalog
```

Let's then list all the keys in our `catalog`:

```{code-cell} ipython3
list(catalog.keys())
```

As you can see, the returned list contains keys to different data resources available: 'credits' and 'projects'.

## Accessing processed data from the data catalog

The processed data can be accessed from the catalog using the `projects` key. Let's take a look at the first few rows of the processed data:

```{code-cell} ipython3
catalog['projects'].describe()
```

As you can see, the processed data is stored in a parquet file format. Let's load the processed data for the date '2024-02-13':

```{code-cell} ipython3
processed_df = catalog['projects'](date='2024-02-13').read()
processed_df.head()
```

We can filter out the data for a specific registry using the `registry` parameter:

```{code-cell} ipython3
processed_df_verra = processed_df[processed_df['registry'] == 'verra']
processed_df_verra.head()
```

```{note}
For detailed information on the transformations and the schema of the processed data, refer to the [Data Processing](data-processing.md) guide and the [API reference](api.md).
```
