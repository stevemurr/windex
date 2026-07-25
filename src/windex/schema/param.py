"""One typed, bounded, self-describing parameter — the shape every form renders.

Three things in this codebase independently grew the same idea: `settings_schema.Field`
(editable settings), `api.jobs.Param` (job arguments), and `crawl.recipe`'s hand-written
clamp helpers (crawl limits). They agree on the important part — *a value is typed,
bounded, and clamped to the operator's ceiling rather than rejected* — and disagree on
everything else, so a client has to know three shapes to render three forms.

This is that idea once. `Param` is the superset; `describe()` is the JSON a client
builds a control from; `coerce()` is the validation, and it is a SECURITY BOUNDARY in
exactly the sense `settings_schema`'s docstring means: an allowlist, never a denylist,
because these values arrive over a LAN-exposed API.

Two rules carried over verbatim because they were learned the hard way:

* **Clamp, don't reject.** A caller may always ask to be slower or smaller; never
  faster or bigger than the operator's bound. Failing a whole form submit over a
  typo'd number is worse than silently honouring the ceiling — but the client is
  told, via `clamp`/`clampNote`, so "I typed 0.5 and got 1.0" is never a mystery.
* **`ceiling` / `floor` name an operator Settings key, not a number.** A recipe
  author writes `hi: 20000`; the operator's `crawl_max_pages_ceiling` is what
  actually caps it. The bound a caller sees is `min(hi, operator_ceiling)`. This is
  what lets an untrusted recipe declare generous bounds safely, and it is why
  ceiling keys are themselves absent from every editable allowlist.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# The closed set of value kinds. Adding one means teaching `coerce`, `json_type`
# and every client's editor switch — deliberately a decision, not a default.
KINDS = (
    "int", "float", "str", "bool", "csv", "choice",
    "date", "url", "url_list", "regex_list", "secret_ref", "duration",
)

# Default UI control per kind. A Param may override via `editor` when the default
# reads wrong (a long `str` wanting a textarea, say).
EDITOR_FOR_KIND = {
    "int": "number", "float": "number", "str": "textfield", "bool": "checkbox",
    "csv": "stringList", "choice": "select", "date": "datepicker", "url": "url",
    "url_list": "stringList", "regex_list": "regexList", "secret_ref": "secret",
    "duration": "duration",
}

# The JSON type a value of each kind serializes as. Note `csv` is a STRING: it is
# stored as the raw comma-separated form that Settings' *_list() helpers parse, and
# rendering it as a list editor is a client-side affordance, not a storage change.
_JSON_TYPE = {
    "int": "integer", "float": "number", "str": "string", "bool": "boolean",
    "csv": "string", "choice": "string", "date": "string", "url": "string",
    "url_list": "array", "regex_list": "array", "secret_ref": "string",
    "duration": "string",
}

_NUMERIC = ("int", "float")
_STRINGY = ("str", "csv", "url", "date", "duration")
_LISTY = ("url_list", "regex_list")


@dataclass(frozen=True)
class Param:
    """One editable value. `key` is the attribute/config name it sets."""

    key: str
    kind: str
    lo: float | None = None
    hi: float | None = None
    choices: tuple[str, ...] = ()
    label: str = ""
    help: str = ""

    # --- value semantics ---
    default: object = None
    # `prefill` is what a form STARTS with, distinct from `default` (what the server
    # does when the key is absent). They differ where the default is unwieldy: the
    # DevDocs slug list has a large default nobody wants pasted into a text field.
    prefill: object = None
    required: bool = False

    # --- operator bounds: a Settings attribute name, resolved at coerce time ---
    ceiling: str | None = None
    floor: str | None = None

    # --- list/string bounds ---
    max_items: int | None = None
    max_len: int | None = None
    pattern: str = ""

    # --- form rendering ---
    editor: str = ""            # defaults from EDITOR_FOR_KIND
    section: str = ""           # groups fields under a heading
    unit: str = ""              # rendered as a suffix ("s", "bytes")
    advanced: bool = False      # render inside a collapsed disclosure
    secret: bool = False        # write-only; never echoed on read
    enum_titles: tuple[str, ...] = ()   # human labels parallel to `choices`

    # --- client behaviour hints ---
    locked_reason: str = ""     # present => render disabled with this explanation
    depends_on: dict | None = None      # {"field": "...", "equals"|"in": ...}
    clamp_note: str = ""        # shown when the value would be clamped

    # How an out-of-range value is handled. "clamp" is the windex default and the
    # right one for settings and recipe config. "reject" is for a value someone
    # typed as an explicit instruction — a job argument — where silently running
    # something other than what was asked is worse than an error. The client needs
    # to know which, or it will helpfully adjust a value the server then refuses.
    enforce: str = "clamp"      # clamp | reject

    # --- recipe-only ---
    stage: str = "runtime"      # install | runtime
    allow: tuple[str, ...] = field(default=())   # secret_ref: nameable operator keys

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"{self.key}: unknown kind {self.kind!r}")

    # ------------------------------------------------------------------ describe

    @property
    def clamp(self) -> str:
        """Which ends of the range are silently enforced: floor | ceiling | both.

        Only meaningful for numerics, and it exists so a client can explain the
        adjustment before the server makes it.
        """
        if self.kind not in _NUMERIC or self.enforce != "clamp":
            return ""
        has_lo = self.lo is not None or self.floor is not None
        has_hi = self.hi is not None or self.ceiling is not None
        if has_lo and has_hi:
            return "both"
        if has_lo:
            return "floor"
        if has_hi:
            return "ceiling"
        return ""

    def to_spec(self) -> dict:
        """The STORAGE form — exactly the declaration, and nothing else.

        Distinct from `describe()` on purpose. `describe()` is for a client
        rendering a control: it resolves bounds against the operator's settings,
        camelCases for JS, and omits `ceiling`/`floor` because those name operator
        keys a client has no business seeing. None of that round-trips. A recipe's
        frozen spec must reconstruct the identical Param or a re-run is not a
        re-run, so this emits the declared values verbatim and omits defaults.
        """
        out = {"key": self.key, "kind": self.kind}
        for attr, default in (
            ("lo", None), ("hi", None), ("choices", ()), ("label", ""), ("help", ""),
            ("default", None), ("prefill", None), ("required", False),
            ("ceiling", None), ("floor", None), ("max_items", None),
            ("max_len", None), ("pattern", ""), ("editor", ""), ("section", ""),
            ("unit", ""), ("advanced", False), ("secret", False),
            ("enum_titles", ()), ("locked_reason", ""), ("depends_on", None),
            ("clamp_note", ""), ("stage", "runtime"), ("allow", ()),
            ("enforce", "clamp"),
        ):
            value = getattr(self, attr)
            if value != default:
                out[attr] = list(value) if isinstance(value, tuple) else value
        return out

    def describe(self, effective: object = None) -> dict:
        """The JSON a client renders a control from.

        `effective` is an optional Settings-like object; when given, `lo`/`hi` are
        reported as the OPERATOR-resolved bounds rather than the declared ones, so
        the form shows the range that will actually be enforced.
        """
        lo, hi = self.bounds(effective)
        out = {
            # --- legacy keys: settings_schema.Field.describe()'s exact shape.
            # api/static/components/sources.js reads these; keep them until the
            # console is deleted and the Swift client is the only reader.
            "key": self.key,
            "kind": self.kind,
            "lo": lo,
            "hi": hi,
            "choices": list(self.choices),
            "label": self.label or self.key,
            "help": self.help,
            # --- the generalized shape ---
            "type": _JSON_TYPE[self.kind],
            "editor": self.editor or EDITOR_FOR_KIND[self.kind],
            "title": self.label or self.key,
            "description": self.help,
            "required": self.required,
            "advanced": self.advanced,
            "secret": self.secret,
            "stage": self.stage,
            "enforce": self.enforce,
        }
        # Omit rather than emit nulls: an absent key means "not applicable here",
        # which is easier for a client to branch on than a null that might be a value.
        if self.default is not None:
            out["default"] = self.default
        if self.prefill is not None:
            out["prefill"] = self.prefill
        if self.section:
            out["section"] = self.section
        if self.unit:
            out["unit"] = self.unit
        if self.enum_titles:
            out["enumTitles"] = list(self.enum_titles)
        if self.max_items is not None:
            out["maxItems"] = self.max_items
        if self.max_len is not None:
            out["maxLength"] = self.max_len
        if self.pattern:
            out["pattern"] = self.pattern
        if self.clamp:
            out["clamp"] = self.clamp
        if self.clamp_note:
            out["clampNote"] = self.clamp_note
        if self.locked_reason:
            out["lockedReason"] = self.locked_reason
        if self.depends_on:
            out["dependsOn"] = dict(self.depends_on)
        if self.allow:
            out["allow"] = list(self.allow)
        return out

    # -------------------------------------------------------------------- bounds

    def bounds(self, effective: object = None) -> tuple[float | None, float | None]:
        """Declared bounds tightened by the operator's ceiling/floor keys.

        A missing Settings attribute is ignored rather than fatal: a recipe naming a
        ceiling key that a later windex removed should lose the extra tightening, not
        refuse to load. The declared `lo`/`hi` still bound it.
        """
        lo, hi = self.lo, self.hi
        if effective is not None:
            if self.floor:
                op = getattr(effective, self.floor, None)
                if op is not None:
                    lo = op if lo is None else max(lo, op)
            if self.ceiling:
                op = getattr(effective, self.ceiling, None)
                if op is not None:
                    hi = op if hi is None else min(hi, op)
        return lo, hi

    # -------------------------------------------------------------------- coerce

    def coerce(self, value, effective: object = None):
        """Validate + coerce + clamp one value. Raises ValueError (routes map to 422).

        Error message wording is load-bearing: `settings_schema`'s tests match on it,
        and it is what a form surfaces to the user.
        """
        key = self.key
        if self.locked_reason and value != self.default:
            # A lock means "you may not CHANGE this", not "this may not appear".
            # Restating the default is not a change — and it has to be allowed, or
            # a spec that materializes defaults (so a frozen run is complete and
            # self-describing) could never be parsed back in.
            raise ValueError(f"{key}: not editable ({self.locked_reason})")

        if self.kind == "bool":
            if not isinstance(value, bool):
                raise ValueError(f"{key}: expected true/false")
            return value

        if self.kind == "choice":
            if value not in self.choices:
                raise ValueError(f"{key}: must be one of {', '.join(self.choices)}")
            return value

        if self.kind == "secret_ref":
            if not isinstance(value, str):
                raise ValueError(f"{key}: expected a string")
            name = value.strip()
            # A secret_ref carries the NAME of an operator-provisioned key, never a
            # value, and only from a declared allowlist — so a recipe can neither
            # smuggle a credential in nor name one it was not offered.
            if self.allow and name not in self.allow:
                raise ValueError(f"{key}: must be one of {', '.join(self.allow)}")
            return name

        if self.kind in _STRINGY:
            if not isinstance(value, str):
                raise ValueError(f"{key}: expected a string")
            if self.kind == "csv":
                # Stored as the raw comma string Settings' *_list() helpers parse;
                # normalizing here is what keeps those helpers correct.
                out = ",".join(p.strip() for p in value.split(",") if p.strip())
            else:
                out = value.strip()
            if self.max_len is not None and len(out) > self.max_len:
                raise ValueError(f"{key}: longer than {self.max_len} chars")
            return out

        if self.kind in _LISTY:
            if isinstance(value, str):
                value = [value]
            if not isinstance(value, list):
                raise ValueError(f"{key}: expected a list of strings")
            if self.max_items is not None and len(value) > self.max_items:
                raise ValueError(f"{key}: at most {self.max_items} items")
            out_list = []
            for item in value:
                if not isinstance(item, str) or not item:
                    raise ValueError(f"{key}: each item must be a non-empty string")
                if self.max_len is not None and len(item) > self.max_len:
                    raise ValueError(
                        f"{key}: item longer than {self.max_len} chars")
                out_list.append(item)
            if self.kind == "regex_list":
                # Compile now so a bad pattern is a 422 at submit rather than an
                # exception inside a worker hours later.
                import re
                for pat in out_list:
                    try:
                        re.compile(pat)
                    except re.error as exc:
                        raise ValueError(f"{key}: invalid regex {pat!r} ({exc})")
            return out_list

        if self.kind in _NUMERIC:
            if isinstance(value, bool):      # bool is an int in Python; not here
                raise ValueError(f"{key}: expected a number")
            try:
                n = int(value) if self.kind == "int" else float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{key}: expected a number")
            lo, hi = self.bounds(effective)
            if self.enforce == "reject":
                if (lo is not None and n < lo) or (hi is not None and n > hi):
                    raise ValueError(f"{key} out of range [{lo}, {hi}]")
                return n
            # Clamp rather than reject: a caller may ask to be gentler, and silently
            # honouring the bound beats failing a form submit over a typo.
            if lo is not None:
                n = max(n, int(lo) if self.kind == "int" else lo)
            if hi is not None:
                n = min(n, int(hi) if self.kind == "int" else hi)
            return n

        raise ValueError(f"{key}: unsupported kind {self.kind!r}")
