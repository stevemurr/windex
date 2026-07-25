import Foundation

/// Which corpus to search. `all` fans out across every source.
///
/// Not an enum: `/v1/search` also accepts any registered custom source name, so
/// the set is open at runtime and a closed enum would make custom sources
/// unrepresentable. The known names are offered as statics for call sites that
/// want them.
public struct SearchSource: Sendable, Hashable, Codable, RawRepresentable,
                            ExpressibleByStringLiteral {
    public let rawValue: String
    public init(rawValue: String) { self.rawValue = rawValue }
    public init(stringLiteral value: String) { self.rawValue = value }
    public init(_ value: String) { self.rawValue = value }

    public static let all: SearchSource = "all"
    public static let news: SearchSource = "news"
    public static let github: SearchSource = "github"
    public static let wiki: SearchSource = "wiki"
    public static let arxiv: SearchSource = "arxiv"
    public static let smallweb: SearchSource = "smallweb"
    public static let docs: SearchSource = "docs"
    public static let hn: SearchSource = "hn"
    public static let hf: SearchSource = "hf"
    public static let memory: SearchSource = "memory"

    /// The built-in sources, in the order the UI should offer them.
    public static let builtIn: [SearchSource] = [
        .all, .news, .github, .wiki, .arxiv, .smallweb, .docs, .hn, .hf, .memory,
    ]
}

/// Retrieval mode. `hybrid` = dense (the user's embedding model) + sparse BM25.
public enum SearchMode: String, Sendable, Hashable, Codable, CaseIterable {
    case hybrid, dense, lexical
}

/// Every filter `/v1/search` accepts.
///
/// Most are source-specific — `minStars` only means something for github,
/// `category` for arXiv — and the server ignores the irrelevant ones rather than
/// erroring, so a UI can keep them in one struct and show only the ones that
/// apply to the selected source.
public struct SearchQuery: Sendable, Hashable {
    public var q: String
    public var source: SearchSource
    /// Server bounds: 1...50.
    public var limit: Int
    public var mode: SearchMode

    public var publishedAfter: Date?
    public var publishedBefore: Date?
    /// github: minimum stars.
    public var minStars: Int?
    /// hn: minimum points.
    public var minPoints: Int?
    public var language: String?
    /// arXiv primary category, e.g. `cs.LG`.
    public var category: String?
    /// Small Web feed host, e.g. `example.com`.
    public var outlet: String?
    /// Docs framework, e.g. `python` or `react`.
    public var framework: String?
    /// HF doc root, e.g. `transformers`.
    public var root: String?
    /// HF page kind: `docs`, `learn` or `blog`.
    public var kind: String?
    /// memory: scope recall to one conversation uuid.
    public var conversationID: String?

    public init(
        q: String,
        source: SearchSource = .all,
        limit: Int = 10,
        mode: SearchMode = .hybrid,
        publishedAfter: Date? = nil,
        publishedBefore: Date? = nil,
        minStars: Int? = nil,
        minPoints: Int? = nil,
        language: String? = nil,
        category: String? = nil,
        outlet: String? = nil,
        framework: String? = nil,
        root: String? = nil,
        kind: String? = nil,
        conversationID: String? = nil
    ) {
        self.q = q
        self.source = source
        self.limit = limit
        self.mode = mode
        self.publishedAfter = publishedAfter
        self.publishedBefore = publishedBefore
        self.minStars = minStars
        self.minPoints = minPoints
        self.language = language
        self.category = category
        self.outlet = outlet
        self.framework = framework
        self.root = root
        self.kind = kind
        self.conversationID = conversationID
    }

    /// Query items in the server's parameter names. Nil filters are omitted
    /// entirely rather than sent empty — `language=` is not the same as absent.
    var queryItems: [URLQueryItem] {
        var items: [URLQueryItem] = [
            .init(name: "q", value: q),
            .init(name: "source", value: source.rawValue),
            .init(name: "limit", value: String(limit)),
            .init(name: "mode", value: mode.rawValue),
        ]
        func add(_ name: String, _ value: String?) {
            guard let value else { return }
            items.append(.init(name: name, value: value))
        }
        let iso = ISO8601DateFormatter()
        add("published_after", publishedAfter.map(iso.string(from:)))
        add("published_before", publishedBefore.map(iso.string(from:)))
        add("min_stars", minStars.map(String.init))
        add("min_points", minPoints.map(String.init))
        add("language", language)
        add("category", category)
        add("outlet", outlet)
        add("framework", framework)
        add("root", root)
        add("kind", kind)
        add("conversation_id", conversationID)
        return items
    }
}

/// One search result.
///
/// `id` and `score` are always present; everything else is a source-specific
/// field the server includes only when non-null (`RESULT_FIELDS` in
/// `api/service.py`), so all of them are optional here. Unrecognised keys are
/// preserved in ``additional`` so a server that adds a field is visible to the
/// UI before this struct is updated.
public struct SearchHit: Sendable, Hashable, Codable, Identifiable {
    /// The stable doc id — `news:<hash>`, `gh:owner/repo`. Public API, safe to
    /// persist and to pass to `/v1/docs/{id}`.
    public let id: String
    public let score: Double

    public let url: String?
    public let title: String?
    public let snippet: String?
    public let source: String?
    public let publishedAt: Date?

    // news / smallweb
    public let outlet: String?
    public let language: String?
    public let lang: String?
    public let incomingLinks: Int?

    // github
    public let stars: Int?
    public let topics: [String]?
    public let pushedAt: Date?

    // arxiv
    public let primaryCategory: String?
    public let categories: [String]?
    public let authors: [String]?

    // docs / hf
    public let framework: String?
    public let version: String?
    public let attribution: String?
    public let root: String?
    public let kind: String?

    // hn
    public let points: Int?
    public let numComments: Int?
    public let author: String?
    public let targetURL: String?

    // memory
    public let conversationID: String?
    public let chunkIndex: Int?

    /// Custom sources: the opaque per-doc blob the pusher attached.
    public let extra: JSONValue?

    /// Any key the server sent that isn't modelled above.
    public let additional: [String: JSONValue]

    // `CaseIterable` here is what powers the `additional` passthrough: the known
    // key set is derived from these cases rather than hand-maintained alongside
    // them, so adding a field above can't leave it double-reported.
    fileprivate enum CodingKeys: String, CodingKey, CaseIterable {
        case id, score, url, title, snippet, source
        case publishedAt = "published_at"
        case outlet, language, lang
        case incomingLinks = "incoming_links"
        case stars, topics
        case pushedAt = "pushed_at"
        case primaryCategory = "primary_category"
        case categories, authors, framework, version, attribution, root, kind
        case points
        case numComments = "num_comments"
        case author
        case targetURL = "target_url"
        case conversationID = "conversation_id"
        case chunkIndex = "chunk_index"
        case extra
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(String.self, forKey: .id)
        score = try c.decode(Double.self, forKey: .score)
        url = try c.decodeIfPresent(String.self, forKey: .url)
        title = try c.decodeIfPresent(String.self, forKey: .title)
        snippet = try c.decodeIfPresent(String.self, forKey: .snippet)
        source = try c.decodeIfPresent(String.self, forKey: .source)
        publishedAt = try c.decodeIfPresent(FlexibleDate.self, forKey: .publishedAt)?.date
        outlet = try c.decodeIfPresent(String.self, forKey: .outlet)
        language = try c.decodeIfPresent(String.self, forKey: .language)
        lang = try c.decodeIfPresent(String.self, forKey: .lang)
        incomingLinks = try c.decodeIfPresent(Int.self, forKey: .incomingLinks)
        stars = try c.decodeIfPresent(Int.self, forKey: .stars)
        topics = try c.decodeIfPresent([String].self, forKey: .topics)
        pushedAt = try c.decodeIfPresent(FlexibleDate.self, forKey: .pushedAt)?.date
        primaryCategory = try c.decodeIfPresent(String.self, forKey: .primaryCategory)
        categories = try c.decodeIfPresent([String].self, forKey: .categories)
        authors = try c.decodeIfPresent([String].self, forKey: .authors)
        framework = try c.decodeIfPresent(String.self, forKey: .framework)
        version = try c.decodeIfPresent(String.self, forKey: .version)
        attribution = try c.decodeIfPresent(String.self, forKey: .attribution)
        root = try c.decodeIfPresent(String.self, forKey: .root)
        kind = try c.decodeIfPresent(String.self, forKey: .kind)
        points = try c.decodeIfPresent(Int.self, forKey: .points)
        numComments = try c.decodeIfPresent(Int.self, forKey: .numComments)
        author = try c.decodeIfPresent(String.self, forKey: .author)
        targetURL = try c.decodeIfPresent(String.self, forKey: .targetURL)
        conversationID = try c.decodeIfPresent(String.self, forKey: .conversationID)
        chunkIndex = try c.decodeIfPresent(Int.self, forKey: .chunkIndex)
        extra = try c.decodeIfPresent(JSONValue.self, forKey: .extra)

        let known = Set(CodingKeys.allCases.map(\.stringValue))
        let raw = try JSONValue(from: decoder).objectValue ?? [:]
        additional = raw.filter { !known.contains($0.key) }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(id, forKey: .id)
        try c.encode(score, forKey: .score)
        try c.encodeIfPresent(url, forKey: .url)
        try c.encodeIfPresent(title, forKey: .title)
        try c.encodeIfPresent(snippet, forKey: .snippet)
        try c.encodeIfPresent(source, forKey: .source)
        try c.encodeIfPresent(outlet, forKey: .outlet)
        try c.encodeIfPresent(language, forKey: .language)
        try c.encodeIfPresent(lang, forKey: .lang)
        try c.encodeIfPresent(incomingLinks, forKey: .incomingLinks)
        try c.encodeIfPresent(stars, forKey: .stars)
        try c.encodeIfPresent(topics, forKey: .topics)
        try c.encodeIfPresent(primaryCategory, forKey: .primaryCategory)
        try c.encodeIfPresent(categories, forKey: .categories)
        try c.encodeIfPresent(authors, forKey: .authors)
        try c.encodeIfPresent(framework, forKey: .framework)
        try c.encodeIfPresent(version, forKey: .version)
        try c.encodeIfPresent(attribution, forKey: .attribution)
        try c.encodeIfPresent(root, forKey: .root)
        try c.encodeIfPresent(kind, forKey: .kind)
        try c.encodeIfPresent(points, forKey: .points)
        try c.encodeIfPresent(numComments, forKey: .numComments)
        try c.encodeIfPresent(author, forKey: .author)
        try c.encodeIfPresent(targetURL, forKey: .targetURL)
        try c.encodeIfPresent(conversationID, forKey: .conversationID)
        try c.encodeIfPresent(chunkIndex, forKey: .chunkIndex)
        try c.encodeIfPresent(extra, forKey: .extra)
    }
}

/// `GET /v1/search`.
public struct SearchResponse: Sendable, Hashable, Codable {
    public let query: String
    public let results: [SearchHit]
    /// The mode actually used. Not always a `SearchMode` raw value: when the
    /// embedder is busy the server degrades and reports the prose string
    /// `"lexical (embedder busy — degraded from hybrid)"`, which is why this is
    /// a `String` and ``isDegraded`` exists.
    public let mode: String
    public let tookMs: Int
    public let timings: [String: Double]

    private enum CodingKeys: String, CodingKey {
        case query, results, mode, timings
        case tookMs = "took_ms"
    }

    /// Whether the search fell back to lexical because the embedder was
    /// saturated. Worth surfacing — the results are real but not dense-ranked.
    public var isDegraded: Bool { SearchMode(rawValue: mode) == nil }
}

/// `GET /v1/docs/{doc_id}` — the full stored document.
///
/// Kept as an open JSON object: the field set is source-dependent (the same
/// heterogeneity as ``SearchHit``) and a document viewer renders what's there.
public struct Document: Sendable, Hashable, Codable, Identifiable {
    public let id: String
    public let fields: [String: JSONValue]

    public init(from decoder: Decoder) throws {
        fields = try JSONValue(from: decoder).objectValue ?? [:]
        // Servers have used both `id` and `doc_id` for this over time; accept
        // either so a doc view never renders without an identity.
        id = fields["id"]?.stringValue ?? fields["doc_id"]?.stringValue ?? ""
    }

    public func encode(to encoder: Encoder) throws {
        try JSONValue.object(fields).encode(to: encoder)
    }

    public subscript(key: String) -> JSONValue? { fields[key] }

    public var title: String? { fields["title"]?.stringValue }
    public var url: String? { fields["url"]?.stringValue }
    public var text: String? { fields["text"]?.stringValue }
    public var source: String? { fields["source"]?.stringValue }
}
