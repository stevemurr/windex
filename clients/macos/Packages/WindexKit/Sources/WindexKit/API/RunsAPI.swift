import Foundation

/// One row from the generic run event feed.
public struct RecipeRunEvent: Codable, Hashable, Sendable {
    public let seq: Int
    public let runID: Int?
    public let taskID: Int?
    public let timestamp: String
    public let level: String
    public let event: String
    public let message: String
    public let data: [String: JSONValue]

    enum CodingKeys: String, CodingKey {
        case seq, level, event, message, data
        case runID = "run_id"
        case taskID = "task_id"
        case timestamp = "ts"
    }
}

/// Typed messages from `/admin/v1/runs/{id}/events/stream`.
public enum RecipeRunUpdate: Sendable {
    case run(RecipeRun)
    case events([RecipeRunEvent])
    case end(cursor: Int)
    case serverError(String)
    case unknown(name: String, data: String)
}

private struct RecipeRunRequest: Codable, Sendable {
    let recipe: String
    let flow: String?
    let params: [String: JSONValue]
    let mode: String
    let priority: Int
    let dedupeKey: String?

    enum CodingKeys: String, CodingKey {
        case recipe, flow, params, mode, priority
        case dedupeKey = "dedupe_key"
    }
}

private struct RecipeRunEventPage: Codable, Sendable {
    let events: [RecipeRunEvent]
    let nextCursor: Int

    enum CodingKeys: String, CodingKey {
        case events
        case nextCursor = "next_cursor"
    }
}

private struct RunStreamEnd: Codable {
    let cursor: Int
}

private struct RunStreamError: Codable {
    let error: String
}

extension WindexClient {

    public func runs(
        recipe: String? = nil,
        source: String? = nil,
        state: String? = nil,
        beforeID: Int? = nil,
        limit: Int = 50
    ) async throws -> [RecipeRun] {
        var query = [URLQueryItem(name: "limit", value: String(limit))]
        if let recipe, !recipe.isEmpty {
            query.append(URLQueryItem(name: "recipe", value: recipe))
        }
        if let source, !source.isEmpty {
            query.append(URLQueryItem(name: "source", value: source))
        }
        if let state, !state.isEmpty {
            query.append(URLQueryItem(name: "state", value: state))
        }
        if let beforeID {
            query.append(URLQueryItem(name: "before_id", value: String(beforeID)))
        }
        let page: RecipeRunList = try await send(
            "GET", "/v1/runs", surface: .admin, query: query,
            as: RecipeRunList.self)
        return page.runs ?? []
    }

    public func run(id: Int, includeSpec: Bool = false) async throws -> RecipeRun {
        let query = includeSpec
            ? [URLQueryItem(name: "include_spec", value: "true")]
            : []
        return try await send(
            "GET", "/v1/runs/\(id)", surface: .admin, query: query,
            as: RecipeRun.self)
    }

    public func createRun(
        recipe: String,
        flow: String? = nil,
        params: [String: JSONValue] = [:],
        dryRun: Bool = false,
        priority: Int = 50,
        dedupeKey: String? = nil
    ) async throws -> RecipeRunQueued {
        try await send(
            "POST", "/v1/runs", surface: .admin,
            body: RecipeRunRequest(
                recipe: recipe, flow: flow, params: params,
                mode: dryRun ? "dry_run" : "run", priority: priority,
                dedupeKey: dedupeKey),
            as: RecipeRunQueued.self)
    }

    public func cancelRun(id: Int) async throws -> ActionResult {
        try await send(
            "POST", "/v1/runs/\(id)/cancel", surface: .admin,
            as: ActionResult.self)
    }

    public func runEvents(id: Int, after: Int = 0,
                          limit: Int = 200) async throws -> [RecipeRunEvent] {
        let page: RecipeRunEventPage = try await send(
            "GET", "/v1/runs/\(id)/events", surface: .admin,
            query: [
                URLQueryItem(name: "after", value: String(after)),
                URLQueryItem(name: "limit", value: String(limit)),
            ],
            as: RecipeRunEventPage.self)
        return page.events
    }

    public func runUpdates(id: Int, after: Int = 0, ticks: Int? = nil) throws
        -> AsyncThrowingStream<RecipeRunUpdate, any Error> {
        var query = [URLQueryItem(name: "after", value: String(after))]
        if let ticks {
            query.append(URLQueryItem(name: "ticks", value: String(ticks)))
        }
        let raw = try events(
            "/v1/runs/\(id)/events/stream", surface: .admin, query: query)

        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await event in raw {
                        switch event.event {
                        case "run":
                            continuation.yield(.run(
                                try event.decode(RecipeRun.self)))
                        case "events":
                            continuation.yield(.events(
                                try event.decode([RecipeRunEvent].self)))
                        case "end":
                            let payload = try event.decode(RunStreamEnd.self)
                            continuation.yield(.end(cursor: payload.cursor))
                        case "error":
                            let payload = try event.decode(RunStreamError.self)
                            continuation.yield(.serverError(payload.error))
                        default:
                            continuation.yield(
                                .unknown(name: event.event, data: event.data))
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}
