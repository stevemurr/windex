import Foundation

/// The module palette and recipe validation — what the graph editor is built on.
extension WindexClient {

    /// Every registered recipe. Built-ins and installed recipes are deliberately
    /// the same object and arrive in one list.
    ///
    /// `includeSpec` is off for list screens: eleven complete DAGs are unnecessary
    /// when the sidebar only needs identity and state.
    public func recipes(includeSpec: Bool = false) async throws -> [Recipe] {
        let query = includeSpec
            ? [URLQueryItem(name: "include_spec", value: "true")]
            : []
        let result: RecipeList = try await send(
            "GET", "/v1/recipes", surface: .admin, query: query,
            as: RecipeList.self)
        return result.recipes ?? []
    }

    /// One recipe with its normalized document and compact per-flow graph.
    public func recipe(named name: String) async throws -> Recipe {
        try await send("GET", "/v1/recipes/\(escape(name))", surface: .admin,
                       as: Recipe.self)
    }

    /// The tasks a run would fan out to, without queueing any work.
    ///
    /// Placement exposes lane, dependencies, preconditions and progress weight
    /// so the editor can explain execution without hardcoding module behavior.
    public func recipeTasks(named name: String,
                            flow: String? = nil) async throws -> RecipeTasks {
        let query = flow.flatMap { $0.isEmpty ? nil : $0 }
            .map { [URLQueryItem(name: "flow", value: $0)] } ?? []
        return try await send(
            "GET", "/v1/recipes/\(escape(name))/tasks", surface: .admin,
            query: query, as: RecipeTasks.self)
    }

    /// Port types, node kinds, and every module's config schema.
    ///
    /// The load-bearing endpoint for this client: the editor renders its palette,
    /// its connection rules and every node inspector from this document, so it
    /// hardcodes no vocabulary and a windex that gains a module needs no client
    /// release. Prefer ``RegistryCache`` over calling this directly — it is
    /// ETag'd precisely so a client keeps a copy.
    public func registry(ifNoneMatch etag: String? = nil) async throws
        -> Conditional<Registry> {
        try await sendConditional("/v1/registry", surface: .admin, etag: etag,
                                  as: Registry.self)
    }

    /// Parse and type-check a recipe.
    ///
    /// Pure server-side — no network, no database, no filesystem — which is what
    /// makes it safe to call on every keystroke, and what separates it from
    /// `preview` (which fetches seeds) and a dry run (which executes the graph).
    public func validateRecipe(_ document: JSONValue) async throws -> ValidationReport {
        try await send("POST", "/v1/recipes/validate", surface: .admin,
                       body: document, as: ValidationReport.self)
    }

    /// Validate and install one inert recipe document.
    public func createRecipe(_ document: JSONValue) async throws -> Recipe {
        try await send("POST", "/v1/recipes", surface: .admin,
                       body: document, as: Recipe.self)
    }

    /// Persist a validated next revision. The path owns identity; the server
    /// rejects a document whose `name` disagrees.
    public func updateRecipe(named name: String,
                             document: JSONValue) async throws -> Recipe {
        try await send("PUT", "/v1/recipes/\(escape(name))", surface: .admin,
                       body: document, as: Recipe.self)
    }
}

/// A locally cached copy of the module registry, revalidated against its ETag.
///
/// Two reasons this exists rather than a plain fetch. The registry is the palette
/// for the whole editor, so re-downloading it on every navigation is waste; and
/// keeping the last good copy on disk is what lets the editor stay usable when
/// the backend blinks — which on a self-hosted LAN box it does, during a restart
/// or a rebuild.
///
/// Stored in Application Support rather than `URLCache` so the copy survives a
/// process restart and can be read before the first request completes.
public actor RegistryCache {

    private struct Entry: Codable {
        let etag: String?
        let registry: Registry
    }

    private let client: WindexClient
    private let fileURL: URL
    private var memory: Entry?

    /// - Parameter fileURL: override for tests; defaults to
    ///   `~/Library/Application Support/windex/registry.json`.
    public init(client: WindexClient, fileURL: URL? = nil) {
        self.client = client
        if let fileURL {
            self.fileURL = fileURL
        } else {
            let base = FileManager.default.urls(for: .applicationSupportDirectory,
                                                in: .userDomainMask).first
                ?? URL(fileURLWithPath: NSTemporaryDirectory())
            self.fileURL = base
                .appendingPathComponent("windex", isDirectory: true)
                .appendingPathComponent("registry.json")
        }
    }

    /// The last known registry, if one was ever stored. Never hits the network,
    /// so a view can render from it immediately at launch.
    public func cached() -> Registry? {
        loadIfNeeded()?.registry
    }

    /// Fetch, revalidating against the stored ETag.
    ///
    /// A 304 keeps the cached copy. A transport failure falls back to it too,
    /// rather than throwing — a stale palette beats an editor that cannot open.
    /// The failure is still surfaced via `wasStale` so the UI can say so.
    @discardableResult
    public func load() async throws -> Registry {
        let entry = loadIfNeeded()
        do {
            switch try await client.registry(ifNoneMatch: entry?.etag) {
            case .notModified:
                guard let entry else {
                    // 304 with nothing cached should be impossible — the request
                    // only carries If-None-Match when there is an entry — but
                    // treat it as a cache miss rather than trusting it.
                    return try await forceLoad()
                }
                wasStale = false
                return entry.registry
            case .modified(let registry, let etag):
                store(Entry(etag: etag, registry: registry))
                wasStale = false
                return registry
            }
        } catch let error as WindexError {
            if case .transport = error, let entry {
                wasStale = true
                return entry.registry
            }
            throw error
        }
    }

    /// Whether the copy last returned came from cache after a failed refresh.
    public private(set) var wasStale = false

    private func forceLoad() async throws -> Registry {
        switch try await client.registry(ifNoneMatch: nil) {
        case .modified(let registry, let etag):
            store(Entry(etag: etag, registry: registry))
            return registry
        case .notModified:
            throw WindexError.http(status: 304,
                                   message: "server reported not-modified with no cached copy")
        }
    }

    private func loadIfNeeded() -> Entry? {
        if let memory { return memory }
        guard let data = try? Data(contentsOf: fileURL),
              let entry = try? JSONDecoder().decode(Entry.self, from: data)
        else { return nil }
        memory = entry
        return entry
    }

    private func store(_ entry: Entry) {
        memory = entry
        // A cache write failing is not worth failing the load over — the value is
        // in memory and the next launch simply refetches.
        try? FileManager.default.createDirectory(
            at: fileURL.deletingLastPathComponent(),
            withIntermediateDirectories: true)
        try? JSONEncoder().encode(entry).write(to: fileURL, options: .atomic)
    }
}
