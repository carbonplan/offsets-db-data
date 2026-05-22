ACR_PREFIX = 'ACR'
ART_PREFIX = 'ART'
CAR_PREFIX = 'CAR'
CCB_PREFIX = 'CCB'
GLD_PREFIX = 'GLD'
ISO_PREFIX = 'ISO'
VCS_PREFIX = 'VCS'

REGISTRY_ABBR_MAP = {
    ACR_PREFIX.lower(): 'american-carbon-registry',
    ART_PREFIX.lower(): 'art-trees',
    CAR_PREFIX.lower(): 'climate-action-reserve',
    CCB_PREFIX.lower(): 'cercarbono',
    GLD_PREFIX.lower(): 'gold-standard',
    ISO_PREFIX.lower(): 'isometric',
    VCS_PREFIX.lower(): 'verra',
}


def get_registry_from_project_id(project_id: str) -> str:
    """
    Retrieve the full registry name from a project ID using a predefined abbreviation mapping.

    Parameters
    ----------
    project_id : str
        The project ID whose registry needs to be identified.

    Returns
    -------
    str
        The full name of the registry corresponding to the abbreviation in the project ID.

    Notes
    -----
    - The function expects the first three characters of the project ID to be the abbreviation of the registry.
    - It uses a predefined mapping (`REGISTRY_ABBR_MAP`) to convert the abbreviation to the full registry name.
    - The project ID is converted to lowercase to ensure case-insensitive matching.
    - The function raises a KeyError if the abbreviation is not found in `REGISTRY_ABBR_MAP`.
    """

    lowered_id = project_id.lower()
    return REGISTRY_ABBR_MAP[lowered_id[:3]]
