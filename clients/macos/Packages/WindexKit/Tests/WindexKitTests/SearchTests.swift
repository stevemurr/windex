import Foundation
import Testing
@testable import WindexKit

@Suite("Search")
struct SearchTests {

    private func makeServer() throws -> MockWindexServer {
        let server = try MockWindexServer()
        server.on("GET /v1/search") { _ in .json(Fixtures.search) }
        server.on("GET /v1/docs/gh:qdrant/qdrant") { _ in .json(Fixtures.document) }
        return server
    }

    @Test("a heterogeneous result set decodes without losing fields")
    func decodesMixedSources() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let response = try await client.search("vector search")

        #expect(response.query == "vector search")
        #expect(response.results.count == 6)
        #expect(response.tookMs == 42)
        #expect(!response.isDegraded)

        let gh = try #require(response.results.first)
        #expect(gh.id == "gh:qdrant/qdrant")
        #expect(gh.stars == 21000)
        #expect(gh.topics == ["vector-search", "rust"])
        #expect(gh.pushedAt != nil)

        let arxiv = try #require(response.results.first { $0.source == "arxiv" })
        #expect(arxiv.primaryCategory == "cs.IR")
        #expect(arxiv.authors == ["A. Author"])

        let hn = try #require(response.results.first { $0.source == "hn" })
        #expect(hn.points == 152)
        #expect(hn.numComments == 43)
        #expect(hn.targetURL == "https://example.org/x")

        let memory = try #require(response.results.first { $0.source == "memory" })
        #expect(memory.chunkIndex == 4)
        #expect(memory.conversationID == "b3f1c2d4-0000-4000-8000-000000000001")
    }

    /// Custom sources attach an opaque blob, and the server may add result fields
    /// before this client models them. Neither should be silently dropped.
    @Test("custom-source extra and unmodelled keys survive")
    func preservesUnknownFields() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let response = try await client.search("vector search")
        let custom = try #require(response.results.first { $0.source == "mydocs" })

        #expect(custom.extra?.objectValue?["team"]?.stringValue == "infra")
        #expect(custom.extra?.objectValue?["pinned"]?.boolValue == true)
        #expect(custom.additional["unmodelled_future_field"]?.stringValue
                == "should survive")
    }

    /// `published_at` arrives tz-aware from some sources, naive from others, and
    /// date-only from arXiv. All three must parse — dropping one silently loses
    /// the date on a whole corpus.
    @Test("timestamps parse in every shape the corpus emits")
    func parsesTimestampVariants() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let response = try await client.search("vector search")

        let aware = try #require(response.results.first { $0.id == "gh:qdrant/qdrant" })
        #expect(aware.pushedAt != nil)                     // +00:00 offset

        let naive = try #require(response.results.first { $0.id == "news:9f2a1c" })
        #expect(naive.publishedAt != nil)                  // no offset

        let dateOnly = try #require(response.results.first { $0.id == "arxiv:2401.00001" })
        #expect(dateOnly.publishedAt != nil)               // date only
    }

    /// Every filter has to reach the wire under the server's parameter name. A
    /// typo here means a filter that silently does nothing — the failure mode is
    /// "results look wrong" with no error anywhere.
    @Test("all filters are sent under their server-side names")
    func sendsEveryFilter() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let query = SearchQuery(
            q: "rust",
            source: .github,
            limit: 25,
            mode: .dense,
            publishedAfter: Date(timeIntervalSince1970: 1_700_000_000),
            publishedBefore: Date(timeIntervalSince1970: 1_800_000_000),
            minStars: 500,
            minPoints: 50,
            language: "en",
            category: "cs.LG",
            outlet: "example.com",
            framework: "python",
            root: "transformers",
            kind: "docs",
            conversationID: "b3f1c2d4-0000-4000-8000-000000000001"
        )
        _ = try await client.search(query)

        let sent = try #require(server.lastRequest)
        #expect(sent.query["q"] == "rust")
        #expect(sent.query["source"] == "github")
        #expect(sent.query["limit"] == "25")
        #expect(sent.query["mode"] == "dense")
        #expect(sent.query["min_stars"] == "500")
        #expect(sent.query["min_points"] == "50")
        #expect(sent.query["language"] == "en")
        #expect(sent.query["category"] == "cs.LG")
        #expect(sent.query["outlet"] == "example.com")
        #expect(sent.query["framework"] == "python")
        #expect(sent.query["root"] == "transformers")
        #expect(sent.query["kind"] == "docs")
        #expect(sent.query["conversation_id"] == "b3f1c2d4-0000-4000-8000-000000000001")
        #expect(sent.query["published_after"]?.hasPrefix("2023-11-14") == true)
        #expect(sent.query["published_before"] != nil)
    }

    /// An unset filter must be absent, not sent empty — `language=` is a
    /// different request from no `language` at all.
    @Test("unset filters are omitted entirely")
    func omitsUnsetFilters() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        _ = try await client.search("plain")

        let sent = try #require(server.lastRequest)
        #expect(sent.query["language"] == nil)
        #expect(sent.query["min_stars"] == nil)
        #expect(sent.query["published_after"] == nil)
        #expect(Set(sent.query.keys) == ["q", "source", "limit", "mode"])
    }

    /// Search is on the agent surface, which is open by design. Attaching a
    /// bearer token there would leak the admin credential to a route that never
    /// needs it.
    @Test("the agent surface carries no admin token")
    func agentSurfaceIsUnauthenticated() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "s3cret")
        _ = try await client.search("anything")

        #expect(server.lastRequest?.header("Authorization") == nil)
        #expect(server.lastRequest?.path == "/v1/search")   // no /admin prefix
    }

    @Test("a degraded search is flagged rather than mis-parsed")
    func degradedModeIsDetected() async throws {
        let server = try MockWindexServer()
        server.on("GET /v1/search") { _ in .json(Fixtures.searchDegraded) }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let response = try await client.search("x")

        #expect(response.isDegraded)
        #expect(response.mode.contains("degraded from hybrid"))
        #expect(response.results.count == 1)
    }

    /// `gh:owner/repo` has both a colon and a slash. The route is `{doc_id:path}`
    /// so the slash passes through, but a doc id that round-trips wrong 404s.
    @Test("a doc id with a colon and slash survives the path")
    func documentIDSurvivesPathEncoding() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let doc = try await client.document(id: "gh:qdrant/qdrant")

        #expect(doc.id == "gh:qdrant/qdrant")
        #expect(doc.title == "qdrant")
        #expect(doc["stars"]?.intValue == 21000)
        #expect(server.lastRequest?.path == "/v1/docs/gh:qdrant/qdrant")
    }

    @Test("an unknown doc id maps to .notFound")
    func unknownDocumentIsNotFound() async throws {
        let server = try makeServer()
        server.on("GET /v1/docs/news:missing") { _ in
            .detail("unknown document id: news:missing", status: 404)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        do {
            _ = try await client.document(id: "news:missing")
            Issue.record("expected a throw")
        } catch let error as WindexError {
            guard case .notFound(let message) = error else {
                Issue.record("expected .notFound, got \(error)")
                return
            }
            #expect(message.contains("news:missing"))
        }
    }

    /// A 422 has to arrive as per-field failures so a form can attach the message
    /// to the offending control instead of showing a wall of JSON.
    @Test("422 decodes into per-field validation failures")
    func validationErrorsAreStructured() async throws {
        let server = try MockWindexServer()
        server.on("GET /v1/search") { _ in
            .json("""
                {"detail":[{"loc":["query","limit"],"msg":"Input should be less than or equal to 50","type":"less_than_equal"}]}
                """, status: 422)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        do {
            _ = try await client.search(SearchQuery(q: "x", limit: 500))
            Issue.record("expected a throw")
        } catch let error as WindexError {
            guard case .validation(let failures, _) = error else {
                Issue.record("expected .validation, got \(error)")
                return
            }
            #expect(failures.count == 1)
            #expect(failures[0].field == "limit")
            #expect(failures[0].msg.contains("less than or equal to 50"))
        }
    }
}
