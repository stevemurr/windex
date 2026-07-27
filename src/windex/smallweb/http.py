"""Reusable robots, per-host pacing, and bounded page fetching."""

import threading
import time
from urllib.parse import urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import httpx
from rich.console import Console

from windex.config import Settings
from windex.smallweb import USER_AGENT

ROBOT_AGENT = "windex"
_console = Console()


class RobotsCache:
    """Thread-safe, expiring robots.txt cache."""

    def __init__(
        self,
        client: httpx.Client,
        ttl: float,
        agent: str = ROBOT_AGENT,
        clock=time.monotonic,
        logger=None,
        user_agent: str = USER_AGENT,
    ):
        self.client = client
        self.ttl = ttl
        self.agent = agent
        self._clock = clock
        self._log = logger or _console.print
        self.user_agent = user_agent
        self._cache: dict[str, tuple[RobotFileParser | None, float]] = {}
        self._lock = threading.Lock()

    def _fetch(
        self,
        scheme: str,
        netloc: str,
    ) -> RobotFileParser | None:
        robots_url = urlunsplit((
            scheme or "https",
            netloc,
            "/robots.txt",
            "",
            "",
        ))
        parser = RobotFileParser()
        try:
            response = self.client.get(
                robots_url,
                headers={"User-Agent": self.user_agent},
            )
        except Exception as exc:  # noqa: BLE001 - failure defaults to allow
            self._log(
                f"[yellow]robots fetch failed for {netloc} "
                f"({exc}); allowing[/yellow]"
            )
            return None
        parser.parse(
            [] if response.status_code >= 400 else response.text.splitlines()
        )
        return parser

    def get(self, url: str) -> RobotFileParser | None:
        parts = urlsplit(url)
        host = parts.netloc.lower()
        now = self._clock()
        with self._lock:
            cached = self._cache.get(host)
            if cached and now - cached[1] < self.ttl:
                return cached[0]
        parser = self._fetch(parts.scheme, parts.netloc)
        with self._lock:
            self._cache[host] = (parser, self._clock())
        return parser

    def allowed(self, url: str) -> bool:
        parser = self.get(url)
        if parser is None:
            return True
        try:
            return parser.can_fetch(self.agent, url)
        except Exception:  # noqa: BLE001 - malformed robots defaults to allow
            return True


class HostRateLimiter:
    """Thread-safe minimum interval between requests to the same host."""

    def __init__(
        self,
        interval: float,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        self.interval = interval
        self._clock = clock
        self._sleep = sleep
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def wait(self, host: str) -> None:
        while True:
            with self._lock:
                now = self._clock()
                previous = self._last.get(host)
                if previous is None or now - previous >= self.interval:
                    self._last[host] = now
                    return
                delay = self.interval - (now - previous)
            self._sleep(delay)


class PageFetcher:
    """Fetch a robots-approved, size- and content-type-bounded page."""

    def __init__(
        self,
        client: httpx.Client,
        settings: Settings,
        *,
        robots_ttl: float | None = None,
        host_interval: float | None = None,
        max_bytes: int | None = None,
        allowed_types: tuple[str, ...] = ("html",),
        limiter: HostRateLimiter | None = None,
        on_response=None,
        user_agent: str = USER_AGENT,
    ):
        self.client = client
        self.user_agent = user_agent
        self.robots = RobotsCache(
            client,
            (
                settings.smallweb_robots_ttl
                if robots_ttl is None
                else robots_ttl
            ),
            user_agent=user_agent,
        )
        self.limiter = limiter or HostRateLimiter(
            settings.smallweb_host_interval
            if host_interval is None
            else host_interval
        )
        self.max_bytes = (
            settings.smallweb_max_page_bytes
            if max_bytes is None
            else max_bytes
        )
        self.allowed_types = allowed_types
        self.on_response = on_response

    def fetch(self, url: str) -> str | None:
        if not self.robots.allowed(url):
            return None
        self.limiter.wait(urlsplit(url).netloc.lower())
        try:
            with self.client.stream(
                "GET",
                url,
                headers={"User-Agent": self.user_agent},
            ) as response:
                if self.on_response is not None:
                    self.on_response(response)
                if response.status_code != 200:
                    return None
                content_type = response.headers.get(
                    "content-type",
                    "",
                ).lower()
                if not any(
                    allowed in content_type
                    for allowed in self.allowed_types
                ):
                    return None
                content_length = response.headers.get("content-length")
                if (
                    content_length
                    and content_length.isdigit()
                    and int(content_length) > self.max_bytes
                ):
                    return None
                body = bytearray()
                for chunk in response.iter_bytes(1 << 16):
                    body.extend(chunk)
                    if len(body) > self.max_bytes:
                        return None
                return body.decode(
                    response.encoding or "utf-8",
                    errors="replace",
                )
        except Exception:  # noqa: BLE001 - a page failure is a skipped item
            return None
