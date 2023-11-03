import typing

import pydantic

RegistryType = typing.Literal[
	'verra',
	'global-carbon-council',
	'gold-standard',
	'art-trees',
	'american-carbon-registry',
	'climate-action-reserve',
]


class Urls(pydantic.BaseModel):
	post_url: pydantic.HttpUrl | None  # for APX
	session_url: pydantic.HttpUrl | None  # for APX
	get_url: pydantic.HttpUrl | None  # for all other registries
	root_url: pydantic.HttpUrl | None
	details_url: pydantic.HttpUrl | None

	@pydantic.root_validator
	def check_exclusivity(cls, values):
		post_url = values.get('post_url')
		session_url = values.get('session_url')
		get_url = values.get('get_url')

		if get_url is None and (post_url is None or session_url is None):
			raise ValueError(
				f'post_url: {post_url} and session_url: {session_url} must be defined together'
			)
		return values

	@pydantic.validator('get_url')
	def check_get_url(cls, v, values):
		if v is not None and (
			values.get('post_url') is not None or values.get('session_url') is not None
		):
			raise ValueError('get_url cannot be defined if post_url and session_url are defined')
		return v


class ConfigItem(pydantic.BaseModel):
	"""Configuration item"""

	name: typing.Literal['projects', 'issuances', 'retirements', 'cancellations', 'transactions']
	urls: Urls
	data: dict | str | None
	headers: dict | None


class Configuration(pydantic.BaseModel):
	projects: ConfigItem | None
	issuances: ConfigItem | None
	retirements: ConfigItem | None
	cancellations: ConfigItem | None
	transactions: ConfigItem | None
