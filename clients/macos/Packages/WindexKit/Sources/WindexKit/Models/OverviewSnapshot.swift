import Foundation

public struct OverviewRunCounts: Codable, Hashable, Sendable {
    public let active: Int
    public let queued: Int
    public let blocked: Int
    public let failed: Int

    public init(active: Int = 0, queued: Int = 0, blocked: Int = 0, failed: Int = 0) {
        self.active = active
        self.queued = queued
        self.blocked = blocked
        self.failed = failed
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

public struct OverviewSourceStatus: Codable, Hashable, Identifiable, Sendable {
    public var id: String { source.name }

    public let source: SourceDeployment
    public let lastSuccess: String?
    public let lastFailure: String?

    public init(
        source: SourceDeployment,
        lastSuccess: String? = nil,
        lastFailure: String? = nil
    ) {
        self.source = source
        self.lastSuccess = lastSuccess
        self.lastFailure = lastFailure
    }
}

/// The all-up projection consumed by Overview. It is a server-owned snapshot,
/// not a set of screen-specific client joins.
public struct OverviewSnapshot: Codable, Hashable, Sendable {
    public let revision: Int64
    public let generatedAt: Date
    public let serviceVersion: String
    public let uptimeSeconds: Int
    public let documentsPerMinute: Double
    public let indexedDocuments: Int
    public let stagedDocuments: Int
    public let pendingEmbedding: Int
    public let runs: OverviewRunCounts
    public let sources: [OverviewSourceStatus]
    public let services: [OverviewServiceStatus]
    public let recentFailures: [OperationalEvent]

    public init(
        revision: Int64,
        generatedAt: Date,
        serviceVersion: String,
        uptimeSeconds: Int,
        documentsPerMinute: Double = 0,
        indexedDocuments: Int = 0,
        stagedDocuments: Int = 0,
        pendingEmbedding: Int = 0,
        runs: OverviewRunCounts = .init(),
        sources: [OverviewSourceStatus] = [],
        services: [OverviewServiceStatus] = [],
        recentFailures: [OperationalEvent] = []
    ) {
        self.revision = revision
        self.generatedAt = generatedAt
        self.serviceVersion = serviceVersion
        self.uptimeSeconds = uptimeSeconds
        self.documentsPerMinute = documentsPerMinute
        self.indexedDocuments = indexedDocuments
        self.stagedDocuments = stagedDocuments
        self.pendingEmbedding = pendingEmbedding
        self.runs = runs
        self.sources = sources
        self.services = services
        self.recentFailures = recentFailures
    }
}
