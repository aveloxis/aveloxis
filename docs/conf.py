# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

project = 'Aveloxis'
copyright = '2026, Sean Goggins, University of Missouri, Derek Howard'
author = 'Sean P. Goggins'

# Version is read from internal/db/version.go — the single source of
# truth (CLAUDE.md rule) — so the docs can never drift from the binary
# again (this field sat at a stale hand-written 0.10.7 for months).
import os
import re

def _aveloxis_version():
    here = os.path.dirname(os.path.abspath(__file__))
    version_go = os.path.join(here, '..', 'internal', 'db', 'version.go')
    try:
        with open(version_go) as f:
            m = re.search(r'ToolVersion = "([^"]+)"', f.read())
            if m:
                return m.group(1)
    except OSError:
        pass
    return 'unknown'

release = _aveloxis_version()
version = release

extensions = [
    'myst_parser',
    'sphinx_rtd_theme',
]

# Support both .rst and .md files
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

# MyST-Parser settings for Markdown support
myst_enable_extensions = [
    'colon_fence',
    'deflist',
    'fieldlist',
    'tasklist',
]
myst_heading_anchors = 3
