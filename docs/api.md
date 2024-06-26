# API Reference

This page provides an autogenerated summary of offsets-db-data's API. For more details and examples, refer to the relevant chapters in the main part of teh documentation.

## Registry Specific Functions

The following functions are specific to a given registry and are grouped under each registry's module. We currently support the following registries:

- [verra](https://registry.verra.org/)
- [gold-standard](https://www.goldstandard.org)
- APX registries
  - [art-trees](https://art.apx.com/)
  - [climate action reserve](https://thereserve2.apx.com)
  - [american carbon registry](https://acr2.apx.com/)

### Verra

```{eval-rst}
.. automodule:: offsets_db_data.vcs
   :members:
   :undoc-members:
   :show-inheritance:
```

### Gold Standard

```{eval-rst}
.. automodule:: offsets_db_data.gld
   :members:
   :undoc-members:
   :show-inheritance:
```

### APX Registries

Functionality for APX registries is currently grouped under the `apx`` module.

```{eval-rst}
.. automodule:: offsets_db_data.apx
   :members:
   :undoc-members:
   :show-inheritance:
```

## ARB Data Functions

The following functions are specific to the [ARB data](https://ww2.arb.ca.gov/our-work/programs/compliance-offset-program/arb-offset-credit-issuance).

```{eval-rst}
.. automodule:: offsets_db_data.arb
   :members:
   :undoc-members:
   :show-inheritance:
```

## Common Functions

The following functions are common to all registries.

```{eval-rst}
.. automodule:: offsets_db_data.common
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: offsets_db_data.credits
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: offsets_db_data.projects
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: offsets_db_data.models
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: offsets_db_data.registry
   :members:
   :undoc-members:
   :show-inheritance:

```
