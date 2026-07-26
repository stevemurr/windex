import Foundation

extension WindexClient {
    public func runs(source: String? = nil, pipeline: String? = nil,
                     state: String? = nil, beforeID: Int? = nil,
                     limit: Int = 100) async throws -> RunsWire {
        var query = [URLQueryItem(name: "limit", value: String(limit))]
        if let source { query.append(.init(name: "source", value: source)) }
        if let pipeline { query.append(.init(name: "pipeline", value: pipeline)) }
        if let state { query.append(.init(name: "state", value: state)) }
        if let beforeID { query.append(.init(name: "before_id", value: String(beforeID))) }
        return try await send("GET", "/v1/runs", surface: .admin,
                              query: query, as: RunsWire.self)
    }
    public func sourceRuns(_ name: String, beforeID: Int? = nil,
                           limit: Int = 100) async throws -> RunsWire {
        var query = [URLQueryItem(name: "limit", value: String(limit))]
        if let beforeID { query.append(.init(name: "before_id", value: String(beforeID))) }
        return try await send("GET", "/v1/sources/\(Self.escapePath(name))/runs",
                              surface: .admin, query: query, as: RunsWire.self)
    }
    public func run(_ id: Int, includeSpec: Bool = false) async throws -> RunWire {
        try await send("GET", "/v1/runs/\(id)", surface: .admin,
                       query: [.init(name: "include_spec", value: String(includeSpec))],
                       as: RunWire.self)
    }
    @discardableResult
    public func cancelRun(_ id: Int) async throws -> ActionWire {
        try await send("POST", "/v1/runs/\(id)/cancel",
                       surface: .admin, as: ActionWire.self)
    }
    /// Re-runs the frozen historical revision/configuration of this exact Run.
    public func rerunFrozen(_ id: Int) async throws -> QueuedRunWire {
        try await send("POST", "/v1/runs/\(id)/rerun",
                       surface: .admin, as: QueuedRunWire.self)
    }
    public func runEvents(_ id: Int, after: Int? = nil,
                          limit: Int = 500) async throws -> RunEventsWire {
        var query = [URLQueryItem(name: "limit", value: String(limit))]
        if let after { query.append(.init(name: "after", value: String(after))) }
        return try await send("GET", "/v1/runs/\(id)/events",
                              surface: .admin, query: query, as: RunEventsWire.self)
    }
    public func runOutputs(_ id: Int) async throws -> RunOutputsWire {
        try await send("GET", "/v1/runs/\(id)/outputs",
                       surface: .admin, as: RunOutputsWire.self)
    }
    public func runArtifact(_ runID: Int, artifactID: String) async throws -> Data {
        try await sendRaw("GET",
                          "/v1/runs/\(runID)/artifacts/\(Self.escapePath(artifactID))",
                          surface: .admin)
    }
}
