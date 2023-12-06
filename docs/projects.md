# Transformation

Project data across all registries are transformed to a common Project schema:

```json
{
  "title": "Project",
  "type": "object",
  "properties": {
    "project_id": {
      "title": "Project Id",
      "description": "Project id used by registry system",
      "type": "string"
    },
    "name": {
      "title": "Name",
      "description": "Name of the project",
      "type": "string"
    },
    "registry": {
      "title": "Registry",
      "description": "Name of the registry",
      "type": "string"
    },
    "proponent": { "title": "Proponent", "type": "string" },
    "protocol": {
      "title": "Protocol",
      "description": "List of protocols",
      "type": "array",
      "items": { "type": "string" }
    },
    "category": {
      "title": "Category",
      "description": "List of categories",
      "type": "array",
      "items": { "type": "string" }
    },
    "status": { "title": "Status", "type": "string" },
    "country": { "title": "Country", "type": "string" },
    "listed_at": {
      "title": "Listed At",
      "description": "Date project was listed",
      "type": "string",
      "format": "date"
    },
    "is_compliance": {
      "title": "Is Compliance",
      "description": "Whether project is compliance project",
      "type": "boolean"
    },
    "retired": {
      "title": "Retired",
      "description": "Total of retired credits",
      "type": "integer"
    },
    "issued": {
      "title": "Issued",
      "description": "Total of issued credits",
      "type": "integer"
    },
    "project_url": {
      "title": "Project Url",
      "description": "URL to project details",
      "type": "string"
    }
  },
  "required": ["project_id", "registry"]
}
```

The majority of project attributes are directly taken from the project data downloaded from each registry.
Table 1 provides the mapping from the raw column names found in downloaded registry data to the OffsetsDB project schema.

|                         | **verra**                 | **climate-action-reserve** | **american-carbon-registry**           | **global-carbon-council**     | **gold-standard**         | **art-trees**               |
| ----------------------- | ------------------------- | -------------------------- | -------------------------------------- | ----------------------------- | ------------------------- | --------------------------- |
| **project_id**          | ID                        | Project ID                 | Project ID                             | project_submission_number     | id                        | Program ID                  |
| **name**                | Name                      | Project Name               | Project Name                           | project_url                   | name                      | Program Name                |
| **protocol**            | Methodology               | Project Type               | Project Methodology/Protocol           | project_methodology           | methodology               | \-                          |
| **category**            | inferred from protocol    | inferred from protocol     | inferred from protocol                 | inferred from protocol        | inferred from protocol    | inferred from protocol      |
| **project_subcategory** | manually assigned         | manually assigned          | manually assigned                      | manually assigned             | manually assigned         | manually assigned           |
| **proponent**           | Proponent                 | Project Owner              | Project Developer                      | project_details:project_owner | project_developer         | Sovereign Program Developer |
| **country**             | Country/Area              | Project Site Country       | Project Site Country                   | project_country               | country                   | Program Country             |
| **status**              | Status                    | Status                     | Derived: voluntary + compliance status | project_status                | status                    | Status                      |
| **listed_at**           | Project Listed Date       | \-                         | \-                                     | \-                            | \-                        | \-                          |
| **commenced_at**        | inferred from credit data | inferred from credit data  | inferred from credit data              | inferred from credit data     | inferred from credit data | inferred from credit data   |

## Normalizing Protocols

There is significant variation in the the raw strings used to describe the protocols associated with each prorject.
That variation exists both within and across registries.
For example, as of the first release of OffsetsDB, we observed 129 unique strings used to describe `ACM0002`, a CDM-era methodology for crediting grid-connected renewable energy projects.
We manually constructed a mapping from observed raw strings to a standarized set of protocol names (see [all-protocol-mapping.json](TK)).
We categorized protocol strings `Other` and `Not Provided` as `unknown`.
In some cases, projects are associated with multiple protocols.
We preserve the one-to-many relationship between projects and protocols in the protocol normalization step.

## Categorizing Protocols and Projects

We manually categorized each protocol contained within `all-protocol-mapping.json`, assigning each protocol a primary category.
OffsetsDB contains the following protocol categories:

- agriculture
- biochar
- cookstoves
- energy-efficiency
- forest
- industrial-gases
- industrial-processes
- land-use
- landfill
- mine-methane
- oil-and-gas
- renewable-energy
- transportation
- unknown
- waste-management

Furthermore, OffsetsDB allows for two additional types of subcategorization: protocol subcategory and project subcategory.
Protocol subcategories derive directly from the protocol itself and apply to all projects developed under that protocol.
For example, `ACM0006` specifies rules for generating electricity from biomass.
From the standpoint of category, projects under `ACM0006` are `renewable-energy` projects.
However, all `ACM0006` projects generate electricity from biomass, meaning we can assign those projects a sub-category of `biomass`.
Some protocols, however, allow for the development of multiple sub-categories of project.
For example, `ACM0002` allows for the development of a whole host of renewable energy projects, from hydropower to wind.
As a reuslt, `ACM0002` projects all fall under the category of `renewable-energy`.
Further subdividing the projects by type, however, requires looking at project paperwork, after which it is possible to manually assign the project a `project_subcategry`.
Project subcategorizations are recorded in `project-sub-categories.json`.

## Other normalizations

### Country

We use the Python package [coutnry_convertor](https://github.com/IndEcol/country_converter) to harmonize country names.

### Status

We harmonize project status codes across all registries, allowing projects to have three categories:

- Listed: project exists and cannot yet be issued credits
- Registered: project exists and can be issued credits
- Completed: project exists and will not receive additional

This simplified set of status codes does not make full use of information provided by about project state.
For example, some registries include finer-grain information about where a project is in the registration and verification process.
We made no attempt to harmonize these extra states across registries, though future versions of might attempt to capture additional details about the status of projects.
