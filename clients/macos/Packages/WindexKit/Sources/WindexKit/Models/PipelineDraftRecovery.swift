import Foundation

public struct RecoveredPipelineDraft: Codable, Hashable, Identifiable, Sendable {
    public var id: String {
        "\(draft.name)@\(baseVersion.map { String($0) } ?? "new")"
    }

    public let draft: PipelineDraft
    public let baseVersion: Int?
    public let baseHash: String?
    public let selectedFlow: String
    public let positions: [String: PipelineNodePosition]
    public let groups: [PipelineLayoutGroup]?
    public let annotations: [PipelineLayoutAnnotation]?
    public let updatedAt: Date

    public init(
        draft: PipelineDraft,
        baseVersion: Int? = nil,
        baseHash: String? = nil,
        selectedFlow: String,
        positions: [String: PipelineNodePosition] = [:],
        groups: [PipelineLayoutGroup]? = nil,
        annotations: [PipelineLayoutAnnotation]? = nil,
        updatedAt: Date = Date()
    ) {
        self.draft = draft
        self.baseVersion = baseVersion
        self.baseHash = baseHash
        self.selectedFlow = selectedFlow
        self.positions = positions
        self.groups = groups
        self.annotations = annotations
        self.updatedAt = updatedAt
    }
}

/// Crash recovery for unpublished semantic drafts. This is intentionally local:
/// published layout is synchronized through the backend and has its own ETag.
public actor PipelineDraftRecoveryStore {
    private let directory: URL
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    public init(directory: URL? = nil) {
        if let directory {
            self.directory = directory
        } else {
            let support = FileManager.default.urls(
                for: .applicationSupportDirectory,
                in: .userDomainMask).first!
            self.directory = support
                .appendingPathComponent("Windex", isDirectory: true)
                .appendingPathComponent("PipelineDrafts", isDirectory: true)
        }
        encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
    }

    public func save(_ value: RecoveredPipelineDraft) throws {
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true)
        try encoder.encode(value).write(to: url(for: value.id), options: .atomic)
    }

    public func load(id: String) throws -> RecoveredPipelineDraft? {
        let url = url(for: id)
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        return try decoder.decode(
            RecoveredPipelineDraft.self,
            from: Data(contentsOf: url))
    }

    public func latest() throws -> RecoveredPipelineDraft? {
        guard FileManager.default.fileExists(atPath: directory.path) else {
            return nil
        }
        return try FileManager.default
            .contentsOfDirectory(
                at: directory,
                includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "json" }
            .compactMap { try? decoder.decode(
                RecoveredPipelineDraft.self,
                from: Data(contentsOf: $0)) }
            .max { $0.updatedAt < $1.updatedAt }
    }

    public func discard(id: String) throws {
        let url = url(for: id)
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        try FileManager.default.removeItem(at: url)
    }

    private func url(for id: String) -> URL {
        let safe = id.unicodeScalars.map { scalar -> Character in
            CharacterSet.alphanumerics.contains(scalar) || scalar == "-" || scalar == "_"
                ? Character(String(scalar)) : "_"
        }
        return directory
            .appendingPathComponent(String(safe))
            .appendingPathExtension("json")
    }
}
