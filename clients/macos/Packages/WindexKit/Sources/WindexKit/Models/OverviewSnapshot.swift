import Foundation

public struct OverviewRunCounts: Codable, Hashable, Sendable {
    public let running: Int
    public let queued: Int
    public let blocked: Int
    public let failed: Int
    public let succeeded: Int
    public let cancelled: Int

    public init(
        running: Int = 0,
        queued: Int = 0,
        blocked: Int = 0,
        failed: Int = 0,
        succeeded: Int = 0,
        cancelled: Int = 0
    ) {
        self.running = running
        self.queued = queued
        self.blocked = blocked
        self.failed = failed
        self.succeeded = succeeded
        self.cancelled = cancelled
    }
}

public struct OverviewServiceStatus: Codable, Hashable, Identifiable, Sendable {
    public var id: String { name }
    public let name: String
    public let available: Bool
    public let detail: String?

    public init(name: String, available: Bool, detail: String? = nil) {
        self.name = name
        self.available = available
        self.detail = detail
    }
}

public enum OverviewModuleLockHealth: String, Codable, Hashable, Sendable {
    case ok
    case degraded
    case error
}

public struct OverviewSourceStatus: Codable, Hashable, Identifiable, Sendable {
    public var id: String { source.name }
    public let source: SourceDeployment
    public let documents: Int
    public let searchable: Int
    public let lastIndexedAt: String?
    public let nextTrigger: String?

    public init(
        source: SourceDeployment,
        documents: Int = 0,
        searchable: Int = 0,
        lastIndexedAt: String? = nil,
        nextTrigger: String? = nil
    ) {
        self.source = source
        self.documents = documents
        self.searchable = searchable
        self.lastIndexedAt = lastIndexedAt
        self.nextTrigger = nextTrigger
    }
}

public struct OverviewWorkerLane: Codable, Hashable, Identifiable, Sendable {
    public var id: String { name }
    public let name: String
    public let states: [String: Int]

    public init(name: String, states: [String: Int]) {
        self.name = name
        self.states = states
    }
}

public struct OverviewBlockedPrecondition: Codable, Hashable, Identifiable, Sendable {
    public var id: String { "\(preconditions.joined(separator: ",")):\(reason ?? "")" }
    public let preconditions: [String]
    public let reason: String?
    public let tasks: Int

    public init(preconditions: [String], reason: String?, tasks: Int) {
        self.preconditions = preconditions
        self.reason = reason
        self.tasks = tasks
    }
}

public struct OverviewRunStatus: Codable, Hashable, Identifiable, Sendable {
    public let id: Int
    public let sourceName: String?
    public let pipelineName: String
    public let pipelineVersion: Int
    public let flowName: String
    public let state: String
    public let progress: Double?
    public let finishedAt: String?
    public let error: String?

    public init(
        id: Int,
        sourceName: String?,
        pipelineName: String,
        pipelineVersion: Int,
        flowName: String,
        state: String,
        progress: Double? = nil,
        finishedAt: String? = nil,
        error: String? = nil
    ) {
        self.id = id
        self.sourceName = sourceName
        self.pipelineName = pipelineName
        self.pipelineVersion = pipelineVersion
        self.flowName = flowName
        self.state = state
        self.progress = progress
        self.finishedAt = finishedAt
        self.error = error
    }
}

public struct OverviewRecentDocument: Codable, Hashable, Identifiable, Sendable {
    public let id: String
    public let source: String
    public let title: String
    public let indexedAt: String

    public init(id: String, source: String, title: String, indexedAt: String) {
        self.id = id
        self.source = source
        self.title = title
        self.indexedAt = indexedAt
    }
}

/// The server-owned all-up epoch-2 projection. Every field here maps to a
/// canonical `/admin/v1/overview` key; absent concepts are not synthesized.
public struct OverviewSnapshot: Codable, Hashable, Sendable {
    public let revision: Int64
    public let generatedAt: Date
    public let documents: Int
    public let searchable: Int
    public let vectors: Int?
    public let indexedLastHour: Int
    public let runs: OverviewRunCounts
    public let sources: [OverviewSourceStatus]
    public let services: [OverviewServiceStatus]
    public let moduleLocks: OverviewModuleLockHealth
    public let strandedSources: [String]
    public let workerLanes: [OverviewWorkerLane]
    public let blockedPreconditions: [OverviewBlockedPrecondition]
    public let activeRuns: [OverviewRunStatus]
    public let recentRuns: [OverviewRunStatus]
    public let recentDocuments: [OverviewRecentDocument]
    public let recentFailures: [OperationalEvent]

    public init(
        revision: Int64,
        generatedAt: Date,
        documents: Int = 0,
        searchable: Int = 0,
        vectors: Int? = nil,
        indexedLastHour: Int = 0,
        runs: OverviewRunCounts = .init(),
        sources: [OverviewSourceStatus] = [],
        services: [OverviewServiceStatus] = [],
        moduleLocks: OverviewModuleLockHealth = .error,
        strandedSources: [String] = [],
        workerLanes: [OverviewWorkerLane] = [],
        blockedPreconditions: [OverviewBlockedPrecondition] = [],
        activeRuns: [OverviewRunStatus] = [],
        recentRuns: [OverviewRunStatus] = [],
        recentDocuments: [OverviewRecentDocument] = [],
        recentFailures: [OperationalEvent] = []
    ) {
        self.revision = revision
        self.generatedAt = generatedAt
        self.documents = documents
        self.searchable = searchable
        self.vectors = vectors
        self.indexedLastHour = indexedLastHour
        self.runs = runs
        self.sources = sources
        self.services = services
        self.moduleLocks = moduleLocks
        self.strandedSources = strandedSources
        self.workerLanes = workerLanes
        self.blockedPreconditions = blockedPreconditions
        self.activeRuns = activeRuns
        self.recentRuns = recentRuns
        self.recentDocuments = recentDocuments
        self.recentFailures = recentFailures
    }
}
