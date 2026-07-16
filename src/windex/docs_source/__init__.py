
# Programming documentation from DevDocs' pre-built bundles
# (https://devdocs.io, freeCodeCamp/devdocs, MIT app; per-docset upstream
# licenses carried in each docset's attribution string — stored and surfaced).
#
# NOTE on naming: the package is `docs_source` so it never collides (mentally
# or literally) with the repo's docs/ directory or the /v1/docs API route,
# but the PUBLIC source name — doc ids (docs:<slug>/<path>), the API `source`
# value, the Qdrant collection/alias — is 'docs'.
#
# DevDocs' CDN rejects some default client User-Agents with a 403; the honest
# descriptive windex UA (same pattern as every other windex source) works.
USER_AGENT = "windex/0.1 (self-hosted search index; +https://github.com/stevemurr/windex)"
