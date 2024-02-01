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

As you can see, the returned list contains keys to different data resources available in the catalog such as 'credits', 'projects', 'raw_projects' and transactions from various registries ('raw_verra_transactions', 'raw_gold_standard_transactions', 'raw_art_trees_transactions', and 'raw_american_carbon_registry_transactions').

Knowing these keys can be quite helpful, as you can tailor your analysis based on the specific data you want to extract - be it raw project data from different registries or transactions from these registries.

In the next section, we'll take a closer look at the `raw_projects` dataset.

## Loading the raw projects data

We can use the `describe` function to get an overview of what each database entails. In this case, we are describing `raw_projects`:

```{code-cell} ipython3
catalog['raw_projects'].describe()
```

This indicates that the package allows you to fetch the raw projects data from the registry of your choice, for any particular date. As per the default parameters, it fetches data from the 'verra' registry, for the date '2024-01-01'.

Let's load the `raw_projects` data for the `registry_name` as 'verra' and `date` as '2024-01-30'.

```{code-cell} ipython3
registry_name = 'verra'
date='2024-01-01'

df = catalog['raw_projects'](registry=registry_name, date=date).read()
```

Let's take a glimpse at the first few rows of our dataframe:

```{code-cell} ipython3
df.head()
```

This displays the first five records of our dataframe, allowing us to get a sense of what our data looks like.

## Data Processing

In the next section, we'll take a look at how to process the data using various functions available in the package. Let's import the necessary functions from the package.

```{code-cell} ipython3
from offsets_db_data.common import (
    CREDIT_SCHEMA_UPATH,
    PROJECT_SCHEMA_UPATH,
    load_column_mapping,
    load_inverted_protocol_mapping,
    load_protocol_mapping,
    load_registry_project_column_mapping
)

from offsets_db_data.vcs import (
    add_vcs_project_id,
    add_vcs_project_url,
    add_vcs_compliance_projects,
)

from offsets_db_data.common import (
    set_registry,
    add_missing_columns,
    convert_to_datetime,
    validate
)

from offsets_db_data.projects import (
    harmonize_country_names,
    harmonize_status_codes,
    map_protocol,
    add_category,
    add_is_compliance_flag
)

from offsets_db_data.models import (
    credit_without_id_schema,
    project_schema
)
```

Now, let's load the registry project mapping and invert it:

```{code-cell} ipython3
registry_project_column_mapping = load_registry_project_column_mapping(
        registry_name=registry_name,
        file_path=PROJECT_SCHEMA_UPATH
    )
inverted_column_mapping = {value: key for key, value in registry_project_column_mapping.items()}
protocol_mapping = load_protocol_mapping()
inverted_protocol_mapping = load_inverted_protocol_mapping()
```

Next we use the various functions we've imported to carry out the preprocessing. This is largely automated and involves renaming columns, setting the registry, harmonizing country names, protocol mapping and adding missing columns among others:

```{code-cell} ipython3
df_transformed = (
    df.rename(columns=inverted_column_mapping)
    .set_registry(registry_name=registry_name)
    .add_vcs_project_id()
    .add_vcs_project_url()
    .harmonize_country_names()
    .harmonize_status_codes()
    .map_protocol(inverted_protocol_mapping=inverted_protocol_mapping)
    .add_category(protocol_mapping=protocol_mapping)
    .add_is_compliance_flag()
    .add_vcs_compliance_projects()
    .add_missing_columns(schema=project_schema)
    .convert_to_datetime(columns=['listed_at'], dayfirst=True)
    .validate(schema=project_schema)
)
```

```{note}
The functions used above and their parameters are well documented in the package's [API reference](api.md). You can refer to the API reference for more details on the registry specific functions and their parameters
```

Let's take a look at the first few rows of our transformed dataframe:

```{code-cell} ipython3
df_transformed.head()
```

## Accessing processed data from the data catalog

The processed data can be accessed from the catalog using the `projects` key. Let's take a look at the first few rows of the processed data:

```{code-cell} ipython3
catalog['projects'].describe()
```

As you can see, the processed data is stored in a parquet file format. Let's load the processed data for the date '2024-01-30':

```{code-cell} ipython3
processed_df = catalog['projects'](date='2024-01-30').read()
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
