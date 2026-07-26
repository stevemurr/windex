import Foundation
import Observation
import WindexKit

enum StoreLoadState: Equatable, Sendable {
    case idle
    case loading
    case loaded
    case failed(String)
}

enum LiveConnectionState: Equatable, Sendable {
    case idle
    case awaitingContract
    case connecting
    case live
    case degraded(String)
}

@MainActor
@Observable
final class RegistryStore {
    private(set) var registry: PipelineRegistry?
    private(set) var state: StoreLoadState = .idle
    private(set) var isStale = false

    private let cache: RegistryCache

    init(client: WindexClient) {
        cache = RegistryCache(client: client)
    }

    func load() async {
        if registry == nil, let cached = await cache.cached() {
            do {
                registry = try cached.pipelineRegistry()
                isStale = true
            } catch {
                // A stale registry from the old contract is not useful to the
                // canonical editor. The network refresh below gets the final say.
            }
        }

        state = .loading
        do {
            let loaded = try await cache.load()
            registry = try loaded.pipelineRegistry()
            isStale = await cache.wasStale
            state = .loaded
        } catch {
            state = .failed("The Module registry could not be loaded.")
        }
    }
}

@MainActor
@Observable
final class PipelineStore {
    private(set) var pipelines: [PipelineSummary] = []
    private(set) var revisions: [String: [PipelineRevision]] = [:]
    private(set) var layouts: [String: PipelineFlowLayout] = [:]
    private(set) var state: StoreLoadState = .idle

    func replace(_ values: [PipelineSummary]) {
        pipelines = values.sorted {
            $0.displayTitle.localizedStandardCompare($1.displayTitle) == .orderedAscending
        }
        state = .loaded
    }

    func replaceRevisions(_ values: [PipelineRevision], for pipeline: String) {
        revisions[pipeline] = values.sorted { $0.reference.version > $1.reference.version }
    }

    func apply(_ layout: PipelineFlowLayout) {
        layouts[Self.layoutKey(
            pipeline: layout.pipeline,
            version: layout.version,
            flow: layout.flow)] = layout
    }

    func layout(pipeline: String, version: Int, flow: String) -> PipelineFlowLayout? {
        layouts[Self.layoutKey(pipeline: pipeline, version: version, flow: flow)]
    }

    private static func layoutKey(pipeline: String, version: Int, flow: String) -> String {
        "\(pipeline)@\(version):\(flow)"
    }
}

@MainActor
@Observable
final class SourceStore {
    private(set) var sources: [SourceDeployment] = []
    private(set) var state: StoreLoadState = .idle

    func replace(_ values: [SourceDeployment]) {
        sources = values.sorted {
            $0.displayTitle.localizedStandardCompare($1.displayTitle) == .orderedAscending
        }
        state = .loaded
    }

    func apply(_ source: SourceDeployment) {
        if let index = sources.firstIndex(where: { $0.name == source.name }) {
            sources[index] = source
        } else {
            sources.append(source)
        }
        sources.sort {
            $0.displayTitle.localizedStandardCompare($1.displayTitle) == .orderedAscending
        }
    }
}

@MainActor
@Observable
final class SharedRunStore {
    private(set) var runs: [SourceRunSummary] = []
    private(set) var state: StoreLoadState = .idle

    func replace(_ values: [SourceRunSummary]) {
        runs = values.sorted { $0.id > $1.id }
        state = .loaded
    }

    func apply(_ run: SourceRunSummary) {
        if let index = runs.firstIndex(where: { $0.id == run.id }) {
            runs[index] = run
        } else {
            runs.append(run)
        }
        runs.sort { $0.id > $1.id }
    }
}

@MainActor
@Observable
final class SharedOverviewStore {
    private(set) var snapshot: OverviewSnapshot?
    private(set) var state: StoreLoadState = .idle

    func apply(_ value: OverviewSnapshot) {
        guard value.revision >= (snapshot?.revision ?? .min) else { return }
        snapshot = value
        state = .loaded
    }
}

@MainActor
@Observable
final class SharedLogStore {
    private var buffer = OperationalEventBuffer()
    var filter = OperationalEventFilter()
    var followsNewest = true
    var selectedSequence: Int64?
    private(set) var connection: LiveConnectionState = .idle

    var events: [OperationalEvent] {
        buffer.values.filter(filter.includes)
    }

    var allEvents: [OperationalEvent] {
        buffer.values
    }

    var selectedEvent: OperationalEvent? {
        guard let selectedSequence else { return nil }
        return buffer.values.first { $0.sequence == selectedSequence }
    }

    func append(_ values: [OperationalEvent]) {
        buffer.append(values)
    }

    func clearLocalView() {
        buffer.clear()
        selectedSequence = nil
    }

    func setConnection(_ value: LiveConnectionState) {
        connection = value
    }
}

@MainActor
@Observable
final class LiveEventHub {
    private(set) var connection: LiveConnectionState = .idle

    /// The multiplexed control-plane stream starts when the backend publishes
    /// the canonical contract epoch and typed event API.
    func start(contractEpoch: Int?) {
        connection = contractEpoch == nil ? .awaitingContract : .connecting
    }

    func stop() {
        connection = .idle
    }
}

/// One connection-scoped owner for all authoritative frontend state.
@MainActor
@Observable
final class BackendSession {
    let draftRecovery = PipelineDraftRecoveryStore()
    let registry: RegistryStore
    let pipelines = PipelineStore()
    let sources = SourceStore()
    let runs = SharedRunStore()
    let overview = SharedOverviewStore()
    let logs = SharedLogStore()
    let events = LiveEventHub()

    let client: WindexClient
    let backend: ConnectedBackend

    private(set) var hasStarted = false

    init(client: WindexClient, backend: ConnectedBackend) {
        self.client = client
        self.backend = backend
        registry = RegistryStore(client: client)
    }

    func start() async {
        guard !hasStarted else { return }
        hasStarted = true
        events.start(contractEpoch: backend.evidence.contractEpoch)
        await registry.load()
    }

    func stop() {
        events.stop()
        hasStarted = false
    }
}
