import intake
import pkg_resources

catalog_file = pkg_resources.resource_filename('offsets_db_data', 'catalog.yaml')
catalog = intake.open_catalog(catalog_file)
