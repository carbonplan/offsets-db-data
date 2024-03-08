# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


import datetime
import sys

import offsets_db_data

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
# sys.path.insert(0, os.path.abspath('.'))
# sys.path.insert(os.path.abspath('..'))

print('python exec:', sys.executable)
print('sys.path:', sys.path)


project = 'offsets-db-data'
copyright = f'{datetime.datetime.now().date().year}, carbonplan'
author = 'carbonplan'
release = f'v{offsets_db_data.__version__}'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'myst_nb',
    # 'sphinxext.opengraph',
    'sphinx_copybutton',
    'sphinx_design',
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx_togglebutton',
]

# MyST config
myst_enable_extensions = ['amsmath', 'colon_fence', 'deflist', 'html_image']
myst_url_schemes = ['http', 'https', 'mailto']

# sphinx-copybutton configurations
copybutton_prompt_text = r'>>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: '
copybutton_prompt_is_regexp = True

nb_execution_mode = 'auto'
nb_execution_timeout = 600
nb_execution_raise_on_error = True
autosummary_generate = True


templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
# Sphinx project configuration
source_suffix = ['.rst', '.md']


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output


html_theme = 'sphinx_book_theme'


html_last_updated_fmt = '%b %d, %Y'

html_title = 'offsets-db-data'


html_theme_options = {
    'repository_url': 'https://github.com/carbonplan/offsets-db-data',
    'repository_branch': 'main',
    'use_repository_button': True,
    'path_to_docs': 'docs',
    'use_edit_page_button': True,
    'use_source_button': True,
    'logo': {
        'image_dark': 'monogram-light-cropped.png',
        'image_light': 'monogram-dark-cropped.png',
    },
}
html_static_path = ['_static']

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
}
