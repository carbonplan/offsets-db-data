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
        return REGISTRY_ABBR_MAP[lowered_id[:3]]
