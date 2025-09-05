"""Configuration file for the Sphinx documentation builder."""

# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

from datetime import datetime

from sphinx.ext import autosummary

from gfw.common.version import __version__


autosummary._encoding = "utf-8"

sys.path.insert(0, os.path.abspath("../../src"))

year = datetime.now().year

project = "gfw-common"
copyright = f"2025-{year}, GFW Engineering Team"
author = "GFW Engineering Team"
release = __version__

rst_prolog = """
.. |version| replace:: {version}
""".format(version=__version__)

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_inline_tabs",
    "sphinx.ext.viewcode",
    "sphinx_design",  # <- this provides grid, card, etc.
    "matplotlib.sphinxext.plot_directive",
]

# Mappings for sphinx.ext.intersphinx. Projects have to have Sphinx-generated doc! (.inv file)
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
}

templates_path = ["_templates"]
exclude_patterns = []

autosummary_generate = True
add_module_names = False  # Remove namespaces from class/method signatures.

autosummary_mock_imports = [  # Exclude these from the documentation.
    "gfw.common.assets",
    "gfw.common.version",
]

autodoc_default_options = {
    "inherited-members": None,
}
autoclass_content = "class"
autodoc_inherit_docstrings = True  # If no docstring, inherit from base class.
autodoc_typehints = "signature"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = "alabaster"
# html_theme = "furo"
html_theme = "pydata_sphinx_theme"
# html_theme = "sphinx_rtd_theme"

htmlhelp_basename = "gfw-common"
html_title = f"gfw-common v{__version__}"

html_static_path = ["_static"]

html_theme_options = {
    "navigation_with_keys": True,
    "show_toc_level": 1,  # controls how many levels of headings appear in the sidebar
}

html_css_files = [
    "gfw-common.css",
]

html_theme_options = {
    "header_links_before_dropdown": 6,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/GlobalFishingWatch/gfw-common",
            "icon": "fa-brands fa-github",
        },
    ],
}
