"""Form-schema primitives shared by settings, job params, and Pipeline Modules.

One `Param` type describes every editable value windex exposes, and one
`describe()` shape is what every client renders a control from. See
`windex.schema.param`.
"""

from windex.schema.param import EDITOR_FOR_KIND, KINDS, Param

__all__ = ["Param", "KINDS", "EDITOR_FOR_KIND"]
