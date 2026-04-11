"""JavaScript literal renderers — identical to TypeScript.

The generated test code is the same (node:test + node:assert), just
without type annotations. The Jinja template handles the syntax
differences; the value/assertion rendering is shared.
"""

from literals.typescript import *  # noqa: F401,F403
