import Foundation

public struct SourceSearchIdentity: Codable, Hashable, Sendable {
    public let searchName: String
    public let idPrefix: String
    public let collectionKey: String
    public let searchProfile: String
    public let includeInAll: Bool

    public init(
        searchName: String,
        idPrefix: String,
        collectionKey: String,
        searchProfile: String,
        includeInAll: Bool
    ) {
        self.searchName = searchName
        self.idPrefix = idPrefix
        self.collectionKey = collectionKey
        self.searchProfile = searchProfile
        self.includeInAll = includeInAll
    }
}

public enum SourceActivityState: String, Codable, Hashable, Sendable {
    case idle
    case queued
    case running
    case blocked
    case succeeded
    case cancelled
    case paused
    case failed
    case archived
}

public struct SourceDocumentCounts: Codable, Hashable, Sendable {
    public let staged: Int
    public let pendingEmbedding: Int
    public let searchable: Int
    public let failed: Int
    public let asOf: String?

    public init(
        staged: Int = 0,
        pendingEmbedding: Int = 0,
        searchable: Int = 0,
        failed: Int = 0,
        asOf: String? = nil
    ) {
        self.staged = staged
        self.pendingEmbedding = pendingEmbedding
        self.searchable = searchable
        self.failed = failed
        self.asOf = asOf
    }
}

public struct SourceRunSummary: Codable, Hashable, Identifiable, Sendable {
    public let id: Int
    public let sourceName: String?
    public let pipeline: PipelineRevisionReference
    public let state: SourceActivityState
    public let progress: Double?
    public let flow: String
    public let queuedAt: String?
    public let finishedAt: String?
    public let error: String?

    public init(
        id: Int,
        sourceName: String? = nil,
        pipeline: PipelineRevisionReference,
        state: SourceActivityState,
        progress: Double? = nil,
        flow: String,
        queuedAt: String? = nil,
        finishedAt: String? = nil,
        error: String? = nil
    ) {
        self.id = id
        self.sourceName = sourceName
        self.pipeline = pipeline
        self.state = state
        self.progress = progress
        self.flow = flow
        self.queuedAt = queuedAt
        self.finishedAt = finishedAt
        self.error = error
    }
}

public struct SourceStatus: Codable, Hashable, Sendable {
    public let activity: SourceActivityState
    public let counts: SourceDocumentCounts
    public let currentRun: SourceRunSummary?
    public let latestRun: SourceRunSummary?
    public let nextTrigger: String?
    public let recentError: String?

    public init(
        activity: SourceActivityState = .idle,
        counts: SourceDocumentCounts = .init(),
        currentRun: SourceRunSummary? = nil,
        latestRun: SourceRunSummary? = nil,
        nextTrigger: String? = nil,
        recentError: String? = nil
    ) {
        self.activity = activity
        self.counts = counts
        self.currentRun = currentRun
        self.latestRun = latestRun
        self.nextTrigger = nextTrigger
        self.recentError = recentError
    }
}

public struct SourceConfiguration: Codable, Hashable, Sendable {
    public let fields: [Param]
    public let configuredValues: [String: JSONValue]
    public let effectiveValues: [String: JSONValue]
    public let origins: [String: String]
    public let missingRequired: [String]
    public let valuesHash: String

    public init(
        fields: [Param] = [],
        configuredValues: [String: JSONValue] = [:],
        effectiveValues: [String: JSONValue] = [:],
        origins: [String: String] = [:],
        missingRequired: [String] = [],
        valuesHash: String = ""
    ) {
        self.fields = fields
        self.configuredValues = configuredValues
        self.effectiveValues = effectiveValues
        self.origins = origins
        self.missingRequired = missingRequired
        self.valuesHash = valuesHash
    }

    public var isReady: Bool {
        missingRequired.isEmpty
    }
}

/// A configured searchable-corpus deployment of one immutable Pipeline revision.
public struct SourceDeployment: Codable, Hashable, Identifiable, Sendable {
    public var id: String { name }

    public let name: String
    public let title: String
    public let description: String
    public let origin: String
    public let pipeline: PipelineRevisionReference
    public let search: SourceSearchIdentity
    public let stateNamespace: String
    public let enabled: Bool
    public let paused: Bool
    public let archived: Bool
    public let generation: Int
    public let configuration: SourceConfiguration
    public let status: SourceStatus

    public init(
        name: String,
        title: String,
        description: String = "",
        origin: String,
        pipeline: PipelineRevisionReference,
        search: SourceSearchIdentity,
        stateNamespace: String,
        enabled: Bool = true,
        paused: Bool = false,
        archived: Bool = false,
        generation: Int = 1,
        configuration: SourceConfiguration = .init(),
        status: SourceStatus = .init()
    ) {
        self.name = name
        self.title = title
        self.description = description
        self.origin = origin
        self.pipeline = pipeline
        self.search = search
        self.stateNamespace = stateNamespace
        self.enabled = enabled
        self.paused = paused
        self.archived = archived
        self.generation = generation
        self.configuration = configuration
        self.status = status
    }

    public var displayTitle: String {
        title.isEmpty ? name : title
    }
}
