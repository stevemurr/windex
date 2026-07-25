import Foundation
import Testing

/// Response bodies for the mock server.
///
/// The settings payloads are GENERATED from windex's own `settings_schema` by
/// `Fixtures/generate_fixtures.py` — hand-writing them would drift from the
/// server and the first symptom would be a form rendering wrong against the real
/// box. The smaller inline fixtures below are transcribed from the route
/// handlers in `api/app.py` and are small enough to keep honest by eye.
enum Fixtures {

    // MARK: - Generated

    static func loadData(_ name: String) throws -> Data {
        guard let url = Bundle.module.url(forResource: "Fixtures/\(name)",
                                          withExtension: nil) else {
            throw FixtureError.missing(name)
        }
        return try Data(contentsOf: url)
    }

    static func load(_ name: String) throws -> String {
        String(decoding: try loadData(name), as: UTF8.self)
    }

    /// `GET /admin/v1/settings` — all 9 scopes, 30 fields, real schema.
    static var allSettings: String { (try? load("settings.json")) ?? "{}" }

    /// scope -> sorted keys, so a schema change fails a test rather than
    /// silently going unrendered.
    static var settingsKeys: [String: [String]] {
        guard let data = try? loadData("settings-keys.json"),
              let decoded = try? JSONDecoder().decode([String: [String]].self, from: data)
        else { return [:] }
        return decoded
    }

    /// One scope pulled out of the generated payload, in the
    /// `{"scope":..., "fields":[...]}` shape `GET /settings/{scope}` returns.
    static func settingsScope(_ scope: String) -> String {
        guard let data = try? loadData("settings.json"),
              let root = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let scopes = root["scopes"] as? [[String: Any]],
              let match = scopes.first(where: { $0["scope"] as? String == scope }),
              let out = try? JSONSerialization.data(withJSONObject: match)
        else { return "{}" }
        return String(decoding: out, as: UTF8.self)
    }

    enum FixtureError: Error { case missing(String) }

    // MARK: - Pairing

    static func health(authRequired: Bool, service: String = "windex") -> String {
        """
        {"status":"ok","service":"\(service)","version":"0.1.0",
         "auth_required":\(authRequired),"started_at":1753372800.0,"uptime_s":128.4}
        """
    }

    static let whoami = """
        {"ok":true,"scopes":["admin"],"auth_required":true}
        """

    // MARK: - Search

    /// A mixed-source result set. Deliberately heterogeneous: a github hit
    /// carries `stars`/`topics`, an arXiv hit `primary_category`/`authors`, an HN
    /// hit `points`, a memory hit `conversation_id`/`chunk_index`, and a custom
    /// source contributes both an `extra` blob and an unmodelled key — which is
    /// exactly the shape `RESULT_FIELDS` produces and what `SearchHit` has to
    /// survive without dropping anything.
    static let search = """
        {
          "query": "vector search",
          "mode": "hybrid",
          "took_ms": 42,
          "timings": {"embed_query_ms": 11.2, "search_ms": 28.9, "total_ms": 42.0},
          "results": [
            {"id": "gh:qdrant/qdrant", "score": 0.9134, "source": "github",
             "url": "https://github.com/qdrant/qdrant", "title": "qdrant",
             "snippet": "Vector database", "stars": 21000,
             "topics": ["vector-search", "rust"], "language": "Rust",
             "pushed_at": "2026-07-20T11:03:00+00:00"},
            {"id": "news:9f2a1c", "score": 0.8210, "source": "news",
             "url": "https://example.com/a", "title": "Search gets cheaper",
             "outlet": "example.com", "lang": "en",
             "published_at": "2026-07-19T08:00:00"},
            {"id": "arxiv:2401.00001", "score": 0.7788, "source": "arxiv",
             "title": "On Hybrid Retrieval", "primary_category": "cs.IR",
             "categories": ["cs.IR", "cs.LG"], "authors": ["A. Author"],
             "published_at": "2026-01-02"},
            {"id": "hn:39000001", "score": 0.7001, "source": "hn",
             "title": "Show HN: a tiny index", "points": 152, "num_comments": 43,
             "author": "someone", "target_url": "https://example.org/x"},
            {"id": "memory:abc-123:4", "score": 0.6553, "source": "memory",
             "snippet": "we talked about qdrant aliases",
             "conversation_id": "b3f1c2d4-0000-4000-8000-000000000001",
             "chunk_index": 4},
            {"id": "custom:thing/1", "score": 0.5120, "source": "mydocs",
             "title": "Internal note", "extra": {"team": "infra", "pinned": true},
             "unmodelled_future_field": "should survive"}
          ]
        }
        """

    /// The degraded shape: `mode` is prose, not a `SearchMode` raw value.
    static let searchDegraded = """
        {"query":"x","mode":"lexical (embedder busy — degraded from hybrid)",
         "took_ms":9,"timings":{"search_ms":9.0,"total_ms":9.0},
         "results":[{"id":"news:1","score":0.4}]}
        """

    static let document = """
        {"id":"gh:qdrant/qdrant","source":"github","title":"qdrant",
         "url":"https://github.com/qdrant/qdrant","text":"Vector database engine.",
         "stars":21000,"topics":["vector-search"]}
        """

    // MARK: - SSE

    /// Hand-framed SSE, split across chunks at awkward boundaries — mid-event and
    /// mid-line — so the parser is tested against arbitrary buffer splits rather
    /// than one tidy write per event. Includes a `:` keep-alive comment and a
    /// multi-line `data:` field, both of which windex emits.
    static let sseChunks: [String] = [
        "event: tick\ndata: {\"n\":1}\n\n",
        ": keep-alive\n\n",
        "event: tick\nda",
        "ta: {\"n\":2}\n\nevent: note\ndata: line one\ndata: line two\n\n",
        "id: 7\nevent: tick\ndata: {\"n\":3}\n\n",
    ]
}
