# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

import datetime
import importlib
import inspect
import os
import subprocess
import sys
from subprocess import CalledProcessError

# -- Project information -----------------------------------------------------

_mod = importlib.import_module("ducklake")

project = "ducklake"
copyright = f"{datetime.date.today().year}, Oliver Borchert"
author = "Oliver Borchert"

extensions = [
    # builtin sphinx
    "sphinx.ext.autosummary",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.linkcode",
    "sphinx.ext.napoleon",
    # external
    "autodocsumm",
    "myst_parser",
    "nbsphinx",
    "numpydoc",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_toolbox.more_autodoc.overloads",
]

## sphinx
# html output
html_theme = "pydata_sphinx_theme"
pygments_style = "lovelace"
html_theme_options = {
    "external_links": [
        {
            "name": "DuckLake",
            "url": "https://ducklake.select",
        },
    ],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/borchero/ducklake-sdk",
            "icon": "fa-brands fa-github",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/ducklake-sdk/",
            "icon": "fa-brands fa-python",
        },
    ],
}
html_title = "DuckLake Python SDK"
html_static_path = []
html_css_files = []
html_show_sourcelink = False

# markup
default_role = "code"

# object signatures
maximum_signature_line_length = 88

# source files
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

# templating
templates_path = ["_templates"]

## sphinx.ext.autodoc
autoclass_content = "both"
autodoc_default_options = {
    "inherited-members": True,
}

## sphinx.ext.intersphinx
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "polars": ("https://docs.pola.rs/py-polars/html/", None),
    "duckdb": ("https://duckdb.org/docs/current/clients/python/reference/", None),
    "pyarrow": ("https://arrow.apache.org/docs/", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
}

## myst_parser
myst_parser_config = {"myst_enable_extensions": ["rst_eval_roles"]}
nitpick_ignore = [("myst", "group-rules")]

## numpydoc
numpydoc_class_members_toctree = False
numpydoc_show_class_members = False

## sphinx_toolbox
overloads_location = ["bottom"]


# Copied and adapted from
# https://github.com/pandas-dev/pandas/blob/4a14d064187367cacab3ff4652a12a0e45d0711b/doc/source/conf.py#L613-L659


## Required configuration function to use sphinx.ext.linkcode
def linkcode_resolve(domain: str, info: dict[str, str]) -> str | None:
    """Determine the URL corresponding to a given Python object."""
    if domain != "py":
        return None

    module_name = info["module"]
    full_name = info["fullname"]

    _submodule = sys.modules.get(module_name)
    if _submodule is None:
        return None

    _object = _submodule
    for _part in full_name.split("."):
        try:
            _object = getattr(_object, _part)
        except AttributeError:
            return None

    try:
        fn = inspect.getsourcefile(inspect.unwrap(_object))  # type: ignore
    except TypeError:
        fn = None
    if not fn:
        return None

    try:
        source, line_number = inspect.getsourcelines(_object)
    except OSError:
        line_number = None

    if line_number:
        linespec = f"#L{line_number}-L{line_number + len(source) - 1}"
    else:
        linespec = ""

    fn = os.path.relpath(fn, start=os.path.dirname(_mod.__file__))

    try:
        # See https://stackoverflow.com/a/21901260
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("ascii").strip()
    except CalledProcessError:
        # If subprocess returns non-zero exit status
        commit = "main"

    return (
        "https://github.com/borchero/ducklake-sdk"
        f"/blob/{commit}/ducklake-python/{_mod.__name__}/{fn}{linespec}"
    )
