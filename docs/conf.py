# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

project = 'Aveloxis'
copyright = '2026, Sean Goggins, University of Missouri, Derek Howard'
author = 'Sean P. Goggins'

# Version is read from internal/db/version.go — the single source of
# truth (house rule) — so the docs can never drift from the binary
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

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

html_theme = 'sphinx_rtd_theme'

# MyST-Parser settings for Markdown support
myst_enable_extensions = [
    'colon_fence',
    'deflist',
    'fieldlist',
    'tasklist',
]
myst_heading_anchors = 4

# Pygments aliases. The docs build runs warnings-as-errors (docs.yml,
# .readthedocs.yaml) and Sphinx wraps every lexer in Pygments'
# raiseonerror filter, so a fence language must exist AND lex cleanly.
#   jsonc -> JsonLexer: Pygments has no 'jsonc' alias (only json5), but
#            JsonLexer already accepts // and /* */ comments; fences stay
#            'jsonc' so GitHub renders the comments natively too.
#   sql   -> PostgresLexer: Postgres-only project; the docs quote real pgx
#            statements ($1 placeholders, ::casts) that the generic
#            SqlLexer rejects.
# Both are consulted before pygments.get_lexer_by_name and still get the
# raiseonerror filter, so genuine syntax errors still fail the build.
# Requires Sphinx >= 4 (class-form add_lexer).
from pygments.lexers.data import JsonLexer
from pygments.lexers.sql import PostgresLexer


def setup(app):
    app.add_lexer('jsonc', JsonLexer)
    app.add_lexer('sql', PostgresLexer)
    return {'parallel_read_safe': True, 'parallel_write_safe': True}
