import Foundation

// The agent-facing API: open, additive-only, and the part of windex that should
// outlive every control-plane churn.
extension WindexClient {

    /// `GET /v1/search`.
    public func search(_ query: SearchQuery) async throws -> SearchResponse {
        try await send("GET", "/v1/search", surface: .agent,
                       query: query.queryItems, as: SearchResponse.self)
    }

    /// Convenience for the common case.
    public func search(_ q: String, source: SearchSource = .all,
                       limit: Int = 10, mode: SearchMode = .hybrid) async throws -> SearchResponse {
        try await search(SearchQuery(q: q, source: source, limit: limit, mode: mode))
    }

    /// `GET /v1/docs/{doc_id}`.
    ///
    /// Doc ids contain characters that must survive the path (`gh:owner/repo` has
    /// both a colon and a slash). The route is declared `{doc_id:path}` so the
    /// slash is meant to pass through unescaped, but the colon and anything
    /// exotic in a custom-source id still needs encoding.
    public func document(id: String) async throws -> Document {
        let escaped = id.addingPercentEncoding(
            withAllowedCharacters: Self.docIDAllowed) ?? id
        return try await send("GET", "/v1/docs/\(escaped)", surface: .agent,
                              as: Document.self)
    }

    /// Path-safe characters for a doc id: the unreserved set plus `/`, which the
    /// `:path` converter consumes as part of the id rather than as a separator.
    static let docIDAllowed: CharacterSet = {
        var set = CharacterSet.alphanumerics
        set.insert(charactersIn: "-._~/")
        return set
    }()
}
