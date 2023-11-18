REGISTRY_ABBR_MAP = {
    'vcs': 'verra',
    'car': 'climate-action-reserve',
    'acr': 'american-carbon-registry',
    'art': 'art-trees',
    'gcc': 'global-carbon-council',
    'gld': 'gold-standard',
}


def get_registry_from_project_id(project_id: str) -> str:
    """Input project id, return string for registry"""
    lowered_id = project_id.lower()
    return REGISTRY_ABBR_MAP[lowered_id[:3]]
