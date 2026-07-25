import Foundation

extension WindexClient {

    /// Port types, node kinds, and every module's config schema.
    ///
    /// The registry is the palette and type system for the pipeline composer.
    /// Prefer ``RegistryCache`` so the editor can render immediately from the
    /// last known registry and revalidate it using its ETag.
    public func registry(ifNoneMatch etag: String? = nil) async throws
        -> Conditional<Registry> {
        try await sendConditional("/v1/registry", surface: .admin, etag: etag,
                                  as: Registry.self)
    }
}

/// A locally cached copy of the module registry, revalidated against its ETag.
///
/// The cache lives in Application Support so the pipeline composer remains
/// usable while a self-hosted backend restarts or is temporarily unavailable.
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

    /// The last known registry, if one was ever stored.
    public func cached() -> Registry? {
        loadIfNeeded()?.registry
    }

    /// Fetch and revalidate the registry, falling back to a stale cached copy
    /// only for transport failures.
    @discardableResult
    public func load() async throws -> Registry {
        let entry = loadIfNeeded()
        do {
            switch try await client.registry(ifNoneMatch: entry?.etag) {
            case .notModified:
                guard let entry else {
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

    /// Whether the value last returned was cached after a failed refresh.
    public private(set) var wasStale = false

    private func forceLoad() async throws -> Registry {
        switch try await client.registry(ifNoneMatch: nil) {
        case .modified(let registry, let etag):
            store(Entry(etag: etag, registry: registry))
            return registry
        case .notModified:
            throw WindexError.http(
                status: 304,
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
        try? FileManager.default.createDirectory(
            at: fileURL.deletingLastPathComponent(),
            withIntermediateDirectories: true)
        try? JSONEncoder().encode(entry).write(to: fileURL, options: .atomic)
    }
}
