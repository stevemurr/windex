import Foundation

/// Ad-hoc crawls: index an arbitrary web cluster from a seed link.
///
/// A crawl runs for minutes, so `start` only QUEUES it — a worker executes it —
/// and progress is followed over ``crawlEvents(runID:)``. That is why `start`
/// answers 202 rather than 200.
extension WindexClient {

    /// Recent crawl runs, newest first. Optionally scoped to one source.
    public func crawlRuns(source: String? = nil,
                          limit: Int? = nil) async throws -> CrawlRunList {
        var query: [URLQueryItem] = []
        if let source, !source.isEmpty {
            query.append(.init(name: "source", value: source))
        }
        if let limit {
            query.append(.init(name: "limit", value: String(limit)))
        }
        return try await send("GET", "/v1/crawl/runs", surface: .admin,
                              query: query, as: CrawlRunList.self)
    }

    /// One run with its per-unit detail — what the Run Monitor renders.
    public func crawlRun(id: String) async throws -> CrawlRunDetail {
        try await send("GET", "/v1/crawl/runs/\(escape(id))", surface: .admin,
                       as: CrawlRunDetail.self)
    }

    /// Queue a crawl. Returns 202 with the run id; the crawl itself has not
    /// started yet. Follow it with ``crawlEvents(runID:)``.
    ///
    /// The response is untyped because the server declares only the 202, with no
    /// schema — the run id is read off the returned object.
    @discardableResult
    public func startCrawl(_ request: CrawlStart) async throws -> JSONValue {
        try await send("POST", "/v1/crawl", surface: .admin,
                       body: request, as: JSONValue.self)
    }

    /// Dry-run the scope rules against the seeds without indexing anything.
    /// Unlike `validate` this does fetch, so it is not a per-keystroke call.
    public func previewCrawl(
        _ request: Components.Schemas.CrawlPreviewInput
    ) async throws -> Components.Schemas.CrawlPreviewOutput {
        try await send("POST", "/v1/crawl/preview", surface: .admin,
                       body: request, as: Components.Schemas.CrawlPreviewOutput.self)
    }

    @discardableResult
    public func cancelCrawl(id: String) async throws -> CrawlCancelled {
        try await send("POST", "/v1/crawl/runs/\(escape(id))/cancel", surface: .admin,
                       as: CrawlCancelled.self)
    }

    /// Live progress for one run. Ends when the run does.
    public func crawlEvents(runID: String) throws
        -> AsyncThrowingStream<SSEEvent, any Error> {
        try events("/v1/crawl/runs/\(escape(runID))/events", surface: .admin)
    }
}
