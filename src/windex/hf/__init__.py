# Hugging Face docs, courses and blog (huggingface.co) — windex's second
# FETCH-based source, and the first pointed at a SINGLE host.
#
# SCOPE (measured, not argued — see docs/huggingface-source.md): the ~4,014
# text-rich pages, i.e. /docs/* + /learn/* (3,175, enumerated from llms.txt) and
# /blog/* (829, enumerated from sitemap-blog.xml). The other ~3.9M pages of the
# site are deliberately OUT:
#   * Spaces extract to 220 chars (client-rendered app mount points),
#   * dataset pages extract the viewer TABLE, not prose (54k chars of column
#     stats — it would poison the index),
#   * /papers/<id> IS the arXiv id and windex already indexes arXiv,
#   * model pages cannot be enumerated (sitemap-models covers a rolling 0.2%).
# Do not widen the frontier to the models/datasets/spaces/papers sitemap shards:
# they are "what changed lately" feeds, and using one as a frontier silently
# indexes a random recent slice while looking like it works.
#
# HF's robots.txt is `User-agent: * / Allow: /` with no Crawl-delay, so
# politeness is ours to define — and HF defines it in headers instead: the
# `pages` bucket governing /docs/*, /blog/* and .md is q=100;w=300 = 1 req/3s.
# See fetch.py: the limiter self-throttles off the live `ratelimit:` counter
# rather than open-loop sleeping. Same honest, descriptive UA as every other
# windex source.
USER_AGENT = "windex/0.1 (self-hosted search index; +https://github.com/stevemurr/windex)"

BASE_URL = "https://huggingface.co"

# Per-root upstream license, recorded exactly as the DevDocs source stores
# `attribution` — surfaced in search payloads, never a licence to republish.
# windex's posture is unchanged and covers every root: store text for snippets +
# embeddings, surface a snippet, always link to the canonical URL, never
# republish bodies. llms.txt is a published invitation for machine consumption;
# it changes the politeness calculus, not the licensing one.
#
# ONLY roots whose upstream license was actually checked appear here. Anything
# else resolves to "" (unknown) rather than a guess: a wrong license string is
# worse than an absent one, and the blog in particular is mixed-authorship
# (HF staff + community/org accounts) with no blanket license.
ROOT_LICENSES = {
    # HF's own libraries — docs live in the library repos, all Apache-2.0.
    "docs/transformers": "Apache-2.0",
    "docs/diffusers": "Apache-2.0",
    "docs/peft": "Apache-2.0",
    "docs/trl": "Apache-2.0",
    "docs/accelerate": "Apache-2.0",
    "docs/datasets": "Apache-2.0",
    "docs/huggingface_hub": "Apache-2.0",
    "docs/tokenizers": "Apache-2.0",
    "docs/safetensors": "Apache-2.0",
    "docs/timm": "Apache-2.0",
    "docs/smolagents": "Apache-2.0",
    "docs/optimum": "Apache-2.0",
    "docs/evaluate": "Apache-2.0",
    "docs/text-generation-inference": "Apache-2.0",
    "docs/text-embeddings-inference": "Apache-2.0",
    "docs/transformers.js": "Apache-2.0",
    "docs/huggingface.js": "MIT",
    "docs/lerobot": "Apache-2.0",
    "docs/autotrain": "Apache-2.0",
}


def license_for(root: str) -> str:
    """Upstream license for a doc root, or "" when it was never checked.

    "" is a real answer, not a failure: it means "link out, snippet only" —
    which is windex's posture for every root anyway.
    """
    return ROOT_LICENSES.get(root, "")
