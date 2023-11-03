import typing

import upath

from offsets_db_data.models import Configuration, RegistryType

REGISTRY_ABBR_MAP = {
	'vcs': 'verra',
	'car': 'climate-action-reserve',
	'acr': 'american-carbon-registry',
	'art': 'art-trees',
	'gcc': 'global-carbon-council',
}


def get_registry_from_project_id(project_id: str) -> str:
	"""Input project id, return string for registry"""
	lowered_id = project_id.lower()

	if lowered_id.startswith('GS'):
		# gs is only registry with 2 character abbr, so just special case it
		# somdeday should probably go in a `project` class
		return 'gold-standard'
	else:
		return REGISTRY_ABBR_MAP.get(lowered_id[:3])


def get_registry_configs(*, config_dir: upath.UPath | None = None) -> dict[str, upath.UPath]:
	"""Get registry configuration files"""
	if config_dir is None:
		# load from default location packaged with the library
		config_dir = upath.UPath(__file__).parent / 'configs'
	config_dir = upath.UPath(config_dir)
	if not (files := sorted(config_dir.glob('*.json'))):
		raise ValueError(f'No JSON files found in {config_dir}')

	return {
		file.stem: file for file in files if file.stem in typing.get_args(RegistryType)
	}  # retrieve the argumens with which the Literal was initialized


def load_registry_config(registry_name: str):
	configs = get_registry_configs()
	if registry_name not in configs:
		raise ValueError(f'No configuration file found for {registry_name}')
	return Configuration.parse_file(configs[registry_name])
