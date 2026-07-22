# Vendored frontend libraries (no build, no npm, no runtime CDN)

Committed ES-module builds, served locally by FastAPI at /static/vendor/.
The page resolves the bare `preact` specifier via an import map in the shell.

| file | package | version | source |
|---|---|---|---|
| preact.module.js | preact | 10.25.4 | https://cdn.jsdelivr.net/npm/preact@10.25.4/dist/preact.module.js |
| hooks.module.js  | preact/hooks | 10.25.4 | https://cdn.jsdelivr.net/npm/preact@10.25.4/hooks/dist/hooks.module.js |
| htm.module.js    | htm | 3.1.1 | https://cdn.jsdelivr.net/npm/htm@3.1.1/dist/htm.module.js |

To update: re-download the pinned URLs above, bump the version, commit the bytes.
Nothing here is fetched at runtime.
