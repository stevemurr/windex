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

    init(client: WindexClient) { cache = RegistryCache(client: client) }

    func load() async {
        if registry == nil, let cached = await cache.cached() {
            registry = try? cached.pipelineRegistry()
            isStale = registry != nil
        }
        state = .loading
        do {
            registry = try await cache.load().pipelineRegistry()
            isStale = await cache.wasStale
            state = .loaded
        } catch {
            state = .failed(error.localizedDescription)
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

    func loading() { state = .loading }
    func fail(_ error: Error) { state = .failed(error.localizedDescription) }
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
        layouts[Self.layoutKey(pipeline: layout.pipeline, version: layout.version,
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
    private(set) var settingsETags: [String: String] = [:]
    private(set) var state: StoreLoadState = .idle
    func loading() { state = .loading }
    func fail(_ error: Error) { state = .failed(error.localizedDescription) }
    func replace(_ values: [SourceDeployment]) {
        sources = values.sorted {
            $0.displayTitle.localizedStandardCompare($1.displayTitle) == .orderedAscending
        }
        state = .loaded
    }
    func setSettingsETag(_ etag: String, for source: String) {
        settingsETags[source] = etag
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
    func loading() { state = .loading }
    func fail(_ error: Error) { state = .failed(error.localizedDescription) }
    func replace(_ values: [SourceRunSummary]) {
        runs = values.sorted { $0.id > $1.id }
        state = .loaded
    }
    func apply(_ run: SourceRunSummary) {
        if let index = runs.firstIndex(where: { $0.id == run.id }) {
            runs[index] = run
        } else { runs.append(run) }
        runs.sort { $0.id > $1.id }
    }
}

@MainActor
@Observable
final class SharedOverviewStore {
    private(set) var snapshot: OverviewSnapshot?
    private(set) var state: StoreLoadState = .idle
    func loading() { state = .loading }
    func fail(_ error: Error) { state = .failed(error.localizedDescription) }
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
    var events: [OperationalEvent] { buffer.values.filter(filter.includes) }
    var allEvents: [OperationalEvent] { buffer.values }
    var newestCursor: Int64? { buffer.newestCursor }
    var selectedEvent: OperationalEvent? {
        guard let selectedSequence else { return nil }
        return buffer.values.first { $0.sequence == selectedSequence }
    }
    func append(_ values: [OperationalEvent]) { buffer.append(values) }
    func clearLocalView() { buffer.clear(); selectedSequence = nil }
    func setConnection(_ value: LiveConnectionState) { connection = value }
}

@MainActor
@Observable
final class LiveEventHub {
    private(set) var connection: LiveConnectionState = .idle
    private(set) var lastEventID: String?
    func setConnection(_ value: LiveConnectionState) { connection = value }
    func advance(_ id: String?) { if let id, !id.isEmpty { lastEventID = id } }
    func stop() { connection = .idle }
}

/// Connection-scoped owner of the authoritative epoch-2 projections.
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
    private var isRefreshing = false
    private var controlTask: Task<Void, Never>?
    private var logTask: Task<Void, Never>?
    private var reconciliationTask: Task<Void, Never>?

    init(client: WindexClient, backend: ConnectedBackend) {
        self.client = client
        self.backend = backend
        registry = RegistryStore(client: client)
    }

    func start() async {
        guard !hasStarted else { return }
        hasStarted = true
        await refreshAll()
        startStreams()
    }

    func foreground() async {
        guard hasStarted else { await start(); return }
        await refreshAll()
        if controlTask == nil || logTask == nil { startStreams() }
    }

    func refreshAll() async {
        guard !isRefreshing else { return }
        isRefreshing = true
        defer { isRefreshing = false }
        await registry.load()
        await loadRuns()
        await loadSources()
        await loadPipelines()
        await loadLogs()
        await loadOverview()
    }

    func stop() {
        controlTask?.cancel()
        logTask?.cancel()
        reconciliationTask?.cancel()
        controlTask = nil
        logTask = nil
        reconciliationTask = nil
        events.stop()
        logs.setConnection(.idle)
        hasStarted = false
    }

    private func loadPipelines() async {
        pipelines.loading()
        do {
            let response = try await client.pipelines()
            let deploymentCounts = Dictionary(grouping: sources.sources,
                                              by: { $0.pipeline.pipeline })
                .mapValues(\.count)
            let summaries = response.pipelines.map {
                $0.summary(deploymentCount: deploymentCounts[$0.name] ?? 0)
            }
            pipelines.replace(summaries)
            for summary in summaries {
                let response = try await client.pipelineRevisions(summary.name)
                let revisions = try response.revisions.map {
                    try $0.revision(title: summary.title, description: summary.description)
                }
                pipelines.replaceRevisions(revisions, for: summary.name)
                if let head = revisions.first {
                    for flow in head.spec.flows {
                        if let wire = try? await client.pipelineLayout(
                            summary.name, version: head.reference.version, flow: flow.name),
                           let layout = try? wire.flowLayout(
                            pipeline: summary.name, version: head.reference.version) {
                            pipelines.apply(layout)
                        }
                    }
                }
            }
        } catch {
            pipelines.fail(error)
        }
    }

    private func loadRuns() async {
        runs.loading()
        do {
            runs.replace(try await client.runs(limit: 250).runs.map {
                try $0.summary()
            })
        } catch {
            runs.fail(error)
        }
    }

    private func loadSources() async {
        sources.loading()
        do {
            let response = try await client.sources()
            var values: [SourceDeployment] = []
            for wire in response.sources {
                let settings = try await client.sourceSettings(wire.name)
                sources.setSettingsETag(settings.etag, for: wire.name)
                let statusWire = try await client.sourceStatus(wire.name)
                let status = try statusWire.status(runs: runs.runs)
                let base = try wire.deployment(status: status)
                let scope = try settings.settingsScope()
                let effective = try settings.values.additionalProperties
                    .decode([String: JSONValue].self)
                values.append(base.withConfiguration(.init(
                    fields: scope.fields.map(\.param),
                    configuredValues: base.configuration.configuredValues,
                    effectiveValues: effective,
                    origins: Dictionary(uniqueKeysWithValues: scope.fields.compactMap { field in
                        field.origin.map { origin in (field.key, origin.rawValue) }
                    }),
                    missingRequired: scope.fields.compactMap {
                        $0.param.required && effective[$0.key] == nil ? $0.key : nil
                    },
                    valuesHash: settings.etag
                )))
            }
            sources.replace(values)
        } catch {
            sources.fail(error)
        }
    }

    private func loadLogs() async {
        do {
            let response = try await client.logEvents(.init(limit: 500))
            logs.append(try response.events.map { try $0.operationalEvent() })
        } catch {
            logs.setConnection(.degraded(error.localizedDescription))
        }
    }

    private func loadOverview() async {
        overview.loading()
        do {
            let response = try await client.overview()
            let failures = logs.allEvents.filter {
                $0.level == .error || $0.level == .critical
            }.suffix(20)
            overview.apply(try response.snapshot(
                sourceDeployments: sources.sources,
                recentFailures: Array(failures)
            ))
        } catch {
            overview.fail(error)
        }
    }

    private func startStreams() {
        controlTask?.cancel()
        logTask?.cancel()
        controlTask = Task { [weak self] in await self?.consumeControlStream() }
        logTask = Task { [weak self] in await self?.consumeLogStream() }
    }

    private func consumeControlStream() async {
        var delay: UInt64 = 250_000_000
        while !Task.isCancelled {
            events.setConnection(.connecting)
            do {
                let stream = try await client.controlEvents(
                    after: Int(events.lastEventID ?? ""),
                    lastEventID: events.lastEventID
                )
                events.setConnection(.live)
                for try await event in stream {
                    guard !Task.isCancelled else { return }
                    events.advance(event.id)
                    scheduleReconciliation()
                }
                delay = 250_000_000
                try? await Task.sleep(nanoseconds: delay)
            } catch {
                guard !Task.isCancelled else { return }
                events.setConnection(.degraded(error.localizedDescription))
                try? await Task.sleep(nanoseconds: delay)
                delay = min(delay * 2, 10_000_000_000)
            }
        }
    }

    private func scheduleReconciliation() {
        guard reconciliationTask == nil else { return }
        reconciliationTask = Task { [weak self] in
            try? await Task.sleep(for: .milliseconds(250))
            guard !Task.isCancelled, let self else { return }
            await self.refreshAll()
            self.reconciliationTask = nil
        }
    }

    private func consumeLogStream() async {
        var cursor = logs.newestCursor.map(String.init)
        var delay: UInt64 = 250_000_000
        while !Task.isCancelled {
            logs.setConnection(.connecting)
            do {
                let stream = try await client.logEventStream(
                    .init(after: cursor.flatMap(Int.init), limit: 500),
                    lastEventID: cursor
                )
                logs.setConnection(.live)
                for try await event in stream {
                    guard !Task.isCancelled else { return }
                    if let value = try? event.decode(OperationalEventWire.self),
                       let decoded = try? value.operationalEvent() {
                        logs.append([decoded])
                        cursor = event.id ?? String(decoded.sequence)
                    } else if let id = event.id {
                        cursor = id
                    }
                }
                delay = 250_000_000
            } catch {
                guard !Task.isCancelled else { return }
                logs.setConnection(.degraded(error.localizedDescription))
                try? await Task.sleep(nanoseconds: delay)
                delay = min(delay * 2, 10_000_000_000)
            }
        }
    }

    // MARK: Mutations reconcile every shared projection

    func publish(draft: PipelineDraft, parent: PipelineRevision?) async throws {
        let validation = try await client.validatePipeline(draft.spec)
        guard validation.valid else {
            throw WindexError.http(
                status: 422,
                message: validation.issues.map {
                    "\($0.path): \($0.message)"
                }.joined(separator: "\n")
            )
        }
        if parent == nil {
            _ = try await client.createPipeline(
                name: draft.name, title: draft.title, description: draft.description,
                spec: draft.spec
            )
        } else {
            _ = try await client.publishPipelineRevision(
                draft.name, spec: draft.spec,
                parentVersion: parent?.reference.version,
                parentHash: parent?.reference.specHash
            )
        }
        await refreshAll()
    }

    func saveLayout(_ layout: PipelineFlowLayout) async throws {
        _ = try await client.putPipelineLayout(layout)
        await refreshAll()
    }

    func saveSourceSettings(_ name: String, values: [String: JSONValue]) async throws {
        guard let etag = sources.settingsETags[name] else {
            throw WindexError.preconditionRequired(message: "Source settings ETag is unavailable.")
        }
        _ = try await client.patchSourceSettings(name, values: values, etag: etag)
        await refreshAll()
    }

    func cancel(runID: Int) async throws {
        _ = try await client.cancelRun(runID)
        await refreshAll()
    }

    func rerunFrozen(runID: Int) async throws {
        _ = try await client.rerunFrozen(runID)
        await refreshAll()
    }

    func runLatest(source: String) async throws {
        _ = try await client.runLatestSource(source)
        await refreshAll()
    }
}

private extension SourceDeployment {
    func withConfiguration(_ configuration: SourceConfiguration) -> SourceDeployment {
        SourceDeployment(
            name: name, title: title, description: description, origin: origin,
            pipeline: pipeline, search: search, stateNamespace: stateNamespace,
            enabled: enabled, paused: paused, archived: archived, generation: generation,
            configuration: configuration, status: status
        )
    }
}
