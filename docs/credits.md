# Credits
The `credits` data reports bulk credit transactions: issuances, retirements, and cancellations.
We first download raw credit transaction data from each of the registries. 
We then apply custom, registry-specific transformations to the data, with the goal of mapping all registry data to a common schema.

## Schema

Credit transactions have the following schema: 

```json
{
  'title': 'Credit',
  'properties': {
    'id': {
      'title': 'Id',
      'type': 'integer'
    },
    'project_id': {
      'title': 'Project ID',
      'description': 'Unique project identifier, by registry',
      'type': 'string'
    },
    'quantity': {
      'title': 'Quantity',
      'description': 'Number of credits',
      'type': 'integer'
    },
    'vintage': {
      'title': 'Vintage',
      'description': 'Vintage year of credits',
      'type': 'integer'
    },
    'transaction_date': {
      'title': 'Transaction Date',
      'description': 'Date of transaction',
      'type': 'string',
      'format': 'date'
    },
    'transaction_type': {
      'title': 'Transaction Type',
      'description': 'Type of transaction (i.e., issuance, retirement)',
      'type': 'string'
    }
  }
}
```
## Downloading raw data
We download a fresh copy of project and transaction data on a daily basis.
While downloading, we make no changes to the raw data provided by the registries.
All data are permanently archived and are made immediately available for download in a publicly available S3 bucket (see Data Access TK).

As with `projects` data, we have no plans to release the code the directly interacts with the registries. 
We made this decision to keep this part of OffsetsDB closed in an effort to limit download requests from the registries.

## Transforming raw data

Nearly the entirety of the code contained within `offsets-db-data` involves registry-specific logic for transforming raw registry data into a common, shared schema.
The logic for transforming the data of each registry is contained within a single file and is denoted by the filename.
For example, the logic involved in transforming Verra data are contained within a file named `vcs.py`.
 
Each registry-specific file contains at least two functions: `process_{registry_abbreviation}_credits` and `process_{registry_abbreviation}_projects`
Those functions, in turn, call a series of additional transformation functions that produce the normalized project and credit data which combine to form OffsetsDB.
These transformation functions tend to be quite small and operate on one or two properties of the raw data. 
To continue with the Verra example, `vcs.py` contains functions with names like `set_vcs_vintage_year` and `generate_vcs_project_ids`.
These functions contain the registry-specific logic needed to map Verra's raw data to a common schema. 

### An example
In practice, replicating the behavior of OffsetsDB should be simple.
Here's an example of using `offsets_db_download` to transform the raw transactions data from Verra into a normalized, analysis ready file:

```python
import pandas as pd
from offsets_db_download import vcs

archive_fname = 's3://carbonplan-offsets-db/raw/2023-12-05/verra/transactions.csv.gz'
raw_credits =  pd.read_csv(archive_fname)
processed_credits =  vcs.process_vcs_credits(raw_credits)
```

Invoking single transformation functions, like `set_vcs_vintage_year` is even more straightforward.
Let's say you want to understand more about how OffsetsDB assigns Verra credits a vintage year.
You can explore the behavior of this single transformation function by calling:

```python
raw_credits.set_vcs_vintage_year(date_column='Vintage End')
```

It's worth noting that we've wrapped all transformation functions using the `pandas_flavor.register_dataframe_method` decorator.
That means that after importing a registry module from `offsets_db_download`, the transformation functions of that module are directly callable by any Pandas dataframe.

## Initial Column Mapping
The initial and perhaps must mundane transformation of OffsetsDB involves mapping properties in the raw data to a common schema.
This step requires constructing a map between the names of properties as they appear in the raw data to the property in OffsetsDB.
For example, the Climate Action Reserve data refers to the property, `project_id`, as `Project ID`. 
The ART registry, however, refers to the same property as `Program ID`.

These column mapping files are stored in `offsets_db_data/configs`.
There is a separate mapping file for `projects` data and `credits` data.
Some properties either aren't included in the raw data or inferring their value requires special processing.
In these cases, a `null` value is recorded in the column mapping files. 

## Protocol Mapping \& Categorization
Offset projects are developed by following a specific set of rules, known as a protocol.
Unfortunately, there is no standardized way of referring to the exact protocol (or protocol version) used to develop an offset project.
Even within the domain of a single registry, references to the exact protocol used to develop a project are often inconsistent. 

OffsetsDB addresses this problem by manually assigning every known protocol string to a common schema. 
Take for example the Clean Development Mechanism protocol AMS-III.D., "Methane recovery in animal manure management systems".
Across all six registries included in OffsetsDB, we identified twenty-two unique strings referring to this single protocol. 
OffsetsDB maps these unique strings, which we refer to as "known strings" to a single reference, `ams-iii-d`. 

We also assign each of these unified protocol references a category.
Those categories include:

- agriculture: offsets derived from the management of farmlands
- cookstoves:  offsets derived from in-home cookstoves that are either more efficient or use cleaner fuels
- forest: offsets derived from the management of forests
- ghg-management: offsets derived from the destruction or elimination of greenhouse gases
- land-use: offsets derived from changes in land-use (e.g., avoided conversion)
- renewable-energy: offsets derived from expanding renewable energy capacity

Data about protocol categories and "known strings" are stored in `offsets_db_data/configs/all-protocol-mapping.json`. 

## Registry specific transformations
Some of the transformations involved in producing OffsetsDB require special knowledge or assumptions about the underlying data.
This section of the documentation highlights some of those special cases. 
For additional context, consult specific function docstrings or reach out TK if something doesn't make sense. 

### American Carbon Registry

Project status: When processing ACR projects, we combine two status properties present in the raw data: `Compliance Program Status (ARB or Ecology)` and `Voluntary Status`.
For compliance projects, we report compliance program status. 
For non-compliance projects, we report voluntary status. 

### Verra
There are several unique aspects of Verra's crediting data that require special consideration.
First, erra is unique amongst the registries included in OffsetsDB in that Verra allows for "rolling" credit issuance.
This allows projects to complete the paperwork and verificaiton processes for credit issuance, but delay the actual issuance event.
This results in ambiguities around the precise timing of credit issuance events, as credits that are eligible to be issued but have not yet been issued, are not publicly reported in the Verra crediting data.
We handle this ambiguity by assuming that the first crediting event, be it an issuance, retirement, or cancellation, on a per-project, per-vintage basis results in issuance of 100 percent of credits eligible to be issued for that project-vintage.
Second, Verra's data does not allow the distinction of retirement events from cancellation events.
We report all Verra retirements and cancellations as `retirement/cancellation`.
Third, Verra allows for the simultaneous issuance of multiple vintages.
We assign all credits from these multi-vintage issuances to the first reported vintage year.

### California Compliance Projects
We treat the California Air Resources Board's [issuance table](https://ww2.arb.ca.gov/resources/documents/arb-offset-credit-issuance-table) as the source of truth for all credits issued and retired by any project developed under an ARB-approved protocol.
```
