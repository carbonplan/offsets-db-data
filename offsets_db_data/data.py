from importlib.resources import files

import intake

catalog_file = files('offsets_db_data').joinpath('catalog.yaml')
catalog = intake.open_catalog(catalog_file)
