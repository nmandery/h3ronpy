from h3ronpy import __version__ as hp_version
import os
from pathlib import Path


# Configuration file for the Sphinx documentation builder.

# -- Project information

project = "h3ronpy"
copyright = "2023, the h3ronpy authors"
author = "Nico Mandery"


release = hp_version
version = hp_version

# -- General configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx_rtd_theme",
    "jupyter_sphinx",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]

# -- Options for HTML output

html_theme = "sphinx_rtd_theme"

# -- Options for EPUB output
epub_show_urls = "footnote"

autodoc_typehints = "both"

os.environ["PROJECT_ROOT"] = str(Path(__file__).parent.parent.parent)
