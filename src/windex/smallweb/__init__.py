
# Small Web is windex's only FETCH-based source: a polite poller of the personal
# blogs on Kagi's curated list. Politeness posture (see poll.py): honor robots.txt,
# a per-host minimum interval, a global concurrency cap, and an HONEST descriptive
# User-Agent with a contact URL — a default python-client UA already drew a 403 in
# sampling. Same UA pattern as the other windex sources. windex links out to the
# blogs (traffic to the small web), it does not republish them.
USER_AGENT = "windex/0.1 (self-hosted search index; +https://github.com/stevemurr/windex)"
