import Foundation

/// Reads for the operational surface — everything the Overview, Sources, Jobs,
/// Loops, Schedule, Activity and Logs screens display.
///
/// All on `.admin`, so `/admin/v1/...` on the wire with a bearer token. The
/// response types are generated from `openapi-admin.json`; see `Wire.swift`.
///
/// Query bounds below mirror the server's `Query(...)` declarations. They are
/// clamped rather than passed through, because the server 422s an out-of-range
/// value and a UI that lets someone drag a slider to 3000 lines should not turn
/// that into an error dialog.
extension WindexClient {

    // MARK: - Pipeline state

    /// Discrete tasks, grouped by category, with their `Param` schemas.
    public func jobs() async throws -> [JobInfo] {
        try await send("GET", "/v1/jobs", surface: .admin, as: [JobInfo].self)
    }

    /// Per-source embed loops plus the global pause flag and watchdog state.
    public func loops() async throws -> LoopsState {
        try await send("GET", "/v1/loops", surface: .admin, as: LoopsState.self)
    }

    /// Per-source indexed/pending counts and last activity.
    public func freshness() async throws -> [SourceFreshness] {
        try await send("GET", "/v1/freshness", surface: .admin,
                       as: [SourceFreshness].self)
    }

    /// Watchable things — actions, loops, services — with running state and a
    /// crash flag. Every `name` here is tailable via ``logTail(name:)``.
    public func activity() async throws -> [ActivityItem] {
        try await send("GET", "/v1/activity", surface: .admin, as: [ActivityItem].self)
    }

    /// Live view of the current extraction batch's workers.
    public func workers() async throws -> WorkersState {
        try await send("GET", "/v1/workers", surface: .admin, as: WorkersState.self)
    }

    /// Per-dataset detail: counts by pipeline status and the content date range.
    /// This is the freshness row-click.
    public func datasetStats(source: String) async throws -> DatasetStats {
        try await send("GET", "/v1/datasets/\(escape(source))/stats",
                       surface: .admin, as: DatasetStats.self)
    }

    // MARK: - Schedule

    public func schedule() async throws -> [ScheduleEntry] {
        try await send("GET", "/v1/schedule", surface: .admin, as: [ScheduleEntry].self)
    }

    // MARK: - Throughput

    /// Per-minute ingest/embed counts over a trailing window. Server bounds:
    /// 5...1440 minutes.
    public func timeseries(minutes: Int = 60) async throws -> [TimeseriesPoint] {
        try await send("GET", "/v1/timeseries", surface: .admin,
                       query: [.init(name: "minutes",
                                     value: String(minutes.clamped(to: 5...1440)))],
                       as: [TimeseriesPoint].self)
    }

    /// Search-performance rollup: latency percentiles and degradation counts.
    /// Server bounds: 1...43200 minutes.
    public func searchMetrics(minutes: Int = 60) async throws -> SearchMetrics {
        try await send("GET", "/v1/metrics", surface: .admin,
                       query: [.init(name: "minutes",
                                     value: String(minutes.clamped(to: 1...43200)))],
                       as: SearchMetrics.self)
    }

    // MARK: - Recent document feeds

    /// Most recently seen documents. Server bounds: 1...100.
    public func recent(limit: Int = 30) async throws -> [RecentDoc] {
        try await recentFeed("/v1/recent", limit: limit)
    }

    /// Recently embedded — landed in Qdrant. Server bounds: 1...100.
    public func recentEmbedded(limit: Int = 25) async throws -> [RecentDoc] {
        try await recentFeed("/v1/recent/embedded", limit: limit)
    }

    /// Recently indexed. Server bounds: 1...100.
    public func recentIndexed(limit: Int = 25) async throws -> [RecentDoc] {
        try await recentFeed("/v1/recent/indexed", limit: limit)
    }

    private func recentFeed(_ path: String, limit: Int) async throws -> [RecentDoc] {
        try await send("GET", path, surface: .admin,
                       query: [.init(name: "limit",
                                     value: String(limit.clamped(to: 1...100)))],
                       as: [RecentDoc].self)
    }

    // MARK: - Logs

    /// The catalogue of tailable logs, with size and availability.
    ///
    /// Most are `available: false` most of the time — a log only exists once its
    /// job has run, and container-hosted ones aren't files at all. That is the
    /// common case, not an error.
    public func logs() async throws -> [LogSource] {
        try await send("GET", "/v1/logs", surface: .admin, as: [LogSource].self)
    }

    /// Tail one log. Server bounds: 1...2000 lines, `grep` at most 200 chars.
    ///
    /// `LogTail.available == false` means the log doesn't exist yet; `truncated`
    /// means the window hit the server's byte cap and older lines were dropped.
    public func logTail(name: String, lines: Int = 200, grep: String? = nil,
                        level: LogLevel? = nil) async throws -> LogTail {
        var query: [URLQueryItem] = [
            .init(name: "lines", value: String(lines.clamped(to: 1...2000))),
        ]
        if let grep, !grep.isEmpty {
            query.append(.init(name: "grep", value: String(grep.prefix(200))))
        }
        if let level {
            query.append(.init(name: "level", value: level.rawValue))
        }
        return try await send("GET", "/v1/logs/\(escape(name))", surface: .admin,
                              query: query, as: LogTail.self)
    }

    /// The levels `/logs/{name}` filters on. Not an enum server-side, but a
    /// closed `Literal` — an unlisted value is a 422.
    public enum LogLevel: String, Sendable, CaseIterable {
        case info, warn, error
    }

    // MARK: - Corpus stats

    /// The big one: document counts by source and status, queue depths, vector
    /// counts, control flags, pipeline stages, the embed breaker and search
    /// performance.
    ///
    /// On the AGENT surface — `/v1/stats` is declared on the main app rather than
    /// the ops router, so unlike everything else here it is open and carries no
    /// token. It is also untyped server-side, hence `JSONValue`: the shape is
    /// nested, source-keyed and changes as sources are added, so a struct would
    /// need regenerating every time someone registers a custom source.
    public func stats() async throws -> JSONValue {
        try await send("GET", "/v1/stats", surface: .agent, as: JSONValue.self)
    }

    // MARK: - Shared

    func escape(_ component: String) -> String {
        component.addingPercentEncoding(
            withAllowedCharacters: .urlPathAllowed) ?? component
    }
}

extension Comparable {
    /// Pull a value into a range. Used for the server's documented query bounds,
    /// so a UI control that allows more than the API does degrades to the cap
    /// instead of a 422.
    func clamped(to range: ClosedRange<Self>) -> Self {
        min(max(self, range.lowerBound), range.upperBound)
    }
}
