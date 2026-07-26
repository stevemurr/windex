import Foundation

public struct LogQuery: Sendable, Hashable {
    public var after: Int?
    public var before: Int?
    public var limit: Int
    public var level: String?
    public var component: String?
    public var source: String?
    public var pipeline: String?
    public var runID: Int?
    public var node: String?
    public var module: String?
    public var text: String?
    public var startedAt: String?
    public var endedAt: String?

    public init(after: Int? = nil, before: Int? = nil, limit: Int = 500,
                level: String? = nil, component: String? = nil,
                source: String? = nil, pipeline: String? = nil,
                runID: Int? = nil, node: String? = nil, module: String? = nil,
                text: String? = nil, startedAt: String? = nil,
                endedAt: String? = nil) {
        self.after = after; self.before = before; self.limit = limit
        self.level = level; self.component = component; self.source = source
        self.pipeline = pipeline; self.runID = runID; self.node = node
        self.module = module; self.text = text; self.startedAt = startedAt
        self.endedAt = endedAt
    }

    var queryItems: [URLQueryItem] {
        var items = [URLQueryItem(name: "limit", value: String(limit))]
        func add(_ name: String, _ value: String?) {
            if let value, !value.isEmpty { items.append(.init(name: name, value: value)) }
        }
        add("after", after.map(String.init)); add("before", before.map(String.init))
        add("level", level); add("component", component); add("source", source)
        add("pipeline", pipeline); add("run_id", runID.map(String.init))
        add("node", node); add("module", module); add("text", text)
        add("started_at", startedAt); add("ended_at", endedAt)
        return items
    }
}

extension LogQuery {
    public init(
        filter: OperationalEventFilter,
        after: Int? = nil,
        before: Int? = nil,
        limit: Int = 500
    ) {
        let formatter = ISO8601DateFormatter()
        self.init(
            after: after,
            before: before,
            limit: min(max(limit, 1), 1_000),
            level: filter.levels.count == 1 ? filter.levels.first?.rawValue : nil,
            component: filter.components.count == 1 ? filter.components.first : nil,
            source: filter.sourceName,
            pipeline: filter.pipelineName,
            runID: filter.runID,
            node: filter.node,
            module: filter.module,
            text: filter.text,
            startedAt: filter.startedAt.map(formatter.string(from:)),
            endedAt: filter.endedAt.map(formatter.string(from:))
        )
    }
}

extension WindexClient {
    public func controlEvents(after: Int? = nil, ticks: Int? = nil,
                              lastEventID: String? = nil) throws
        -> AsyncThrowingStream<SSEEvent, any Error> {
        var query: [URLQueryItem] = []
        if let after { query.append(.init(name: "after", value: String(after))) }
        if let ticks { query.append(.init(name: "ticks", value: String(ticks))) }
        return try events("/v1/events/stream", surface: .admin, query: query,
                          lastEventID: lastEventID)
    }

    public func logEvents(_ query: LogQuery = .init()) async throws -> LogEventsWire {
        try await send("GET", "/v1/log-events", surface: .admin,
                       query: query.queryItems, as: LogEventsWire.self)
    }

    public func logFacets() async throws -> LogFacetsWire {
        try await send("GET", "/v1/log-events/facets", surface: .admin,
                       as: LogFacetsWire.self)
    }

    public func logEventStream(_ query: LogQuery = .init(),
                               ticks: Int? = nil,
                               lastEventID: String? = nil) throws
        -> AsyncThrowingStream<SSEEvent, any Error> {
        var items = query.queryItems.filter { $0.name != "limit" && $0.name != "before" }
        if let ticks { items.append(.init(name: "ticks", value: String(ticks))) }
        return try events("/v1/log-events/stream", surface: .admin, query: items,
                          lastEventID: lastEventID)
    }
}
