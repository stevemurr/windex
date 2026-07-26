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
    private(set) var triggers: [String: [SourceTriggerWire]] = [:]
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
    func setTriggers(_ values: [SourceTriggerWire], for source: String) {
        triggers[source] = values.sorted {
            ($0.nextFireAt ?? "\u{10ffff}") < ($1.nextFireAt ?? "\u{10ffff}")
        }
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
    private(set) var details: [Int: RunWire] = [:]
    private(set) var events: [Int: [OperationalEvent]] = [:]
    private(set) var outputs: [Int: [RunOutputWire]] = [:]
    private(set) var detailErrors: [Int: String] = [:]
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
    func applyDetail(
        _ detail: RunWire,
        events eventValues: [OperationalEvent],
        outputs outputValues: [RunOutputWire]
    ) {
        details[detail.id] = detail
        events[detail.id] = eventValues
        outputs[detail.id] = outputValues
        detailErrors.removeValue(forKey: detail.id)
    }
    func failDetail(_ id: Int, error: Error) {
        detailErrors[id] = error.localizedDescription
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
    private var pollingTask: Task<Void, Never>?

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
        pollingTask?.cancel()
        controlTask = nil
        logTask = nil
        reconciliationTask = nil
        pollingTask = nil
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
            runs.replace(try await client.runs(limit: 200).runs.map {
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
                let detailedWire = (try? await client.source(wire.name)) ?? wire
                let settings = try await client.sourceSettings(wire.name)
                sources.setSettingsETag(settings.etag, for: wire.name)
                let triggerResponse = try await client.sourceTriggers(wire.name)
                sources.setTriggers(triggerResponse.triggers, for: wire.name)
                let nextTrigger = triggerResponse.triggers
                    .filter(\.enabled)
                    .compactMap(\.nextFireAt)
                    .sorted()
                    .first
                let statusWire = try await client.sourceStatus(wire.name)
                let status = try statusWire.status(
                    runs: runs.runs,
                    nextTrigger: nextTrigger
                )
                let base = try detailedWire.deployment(status: status)
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
                updatePollingState()
                for try await event in stream {
                    guard !Task.isCancelled else { return }
                    events.advance(event.id)
                    scheduleReconciliation()
                }
                events.setConnection(.degraded("Control stream ended; using REST reconciliation."))
                updatePollingState()
                delay = 250_000_000
                try? await Task.sleep(nanoseconds: delay)
            } catch {
                guard !Task.isCancelled else { return }
                events.setConnection(.degraded(error.localizedDescription))
                updatePollingState()
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
                updatePollingState()
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
                logs.setConnection(.degraded("Log stream ended; using REST reconciliation."))
                updatePollingState()
                delay = 250_000_000
            } catch {
                guard !Task.isCancelled else { return }
                logs.setConnection(.degraded(error.localizedDescription))
                updatePollingState()
                try? await Task.sleep(nanoseconds: delay)
                delay = min(delay * 2, 10_000_000_000)
            }
        }
    }

    private var streamsAreDegraded: Bool {
        if case .degraded = events.connection { return true }
        if case .degraded = logs.connection { return true }
        return false
    }

    /// SSE is primary. While either stream is unavailable, reconcile through
    /// REST at a bounded 2–15 second cadence until both streams are live again.
    private func updatePollingState() {
        guard hasStarted, streamsAreDegraded else {
            pollingTask?.cancel()
            pollingTask = nil
            return
        }
        guard pollingTask == nil else { return }
        pollingTask = Task { [weak self] in
            var delay = Duration.seconds(2)
            while !Task.isCancelled, let self, self.streamsAreDegraded {
                await self.refreshAll()
                try? await Task.sleep(for: delay)
                delay = min(delay * 2, .seconds(15))
            }
            self?.pollingTask = nil
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

    func loadLayout(pipeline: String, version: Int, flow: String) async {
        guard let wire = try? await client.pipelineLayout(
            pipeline,
            version: version,
            flow: flow
        ), let layout = try? wire.flowLayout(pipeline: pipeline, version: version)
        else { return }
        pipelines.apply(layout)
    }

    func saveSourceSettings(_ name: String, values: [String: JSONValue]) async throws {
        guard let etag = sources.settingsETags[name] else {
            throw WindexError.preconditionRequired(message: "Source settings ETag is unavailable.")
        }
        _ = try await client.patchSourceSettings(name, values: values, etag: etag)
        await refreshAll()
    }

    func sourceSettings(_ name: String) async throws -> SourceSettingsWire {
        let response = try await client.sourceSettings(name)
        sources.setSettingsETag(response.etag, for: name)
        return response
    }

    func createSource(_ request: SourceCreateRequest) async throws {
        let validation = try await client.validateSource(request)
        guard validation.valid else {
            throw WindexError.http(
                status: 422,
                message: validation.issues.map {
                    "\($0.path): \($0.message)"
                }.joined(separator: "\n")
            )
        }
        _ = try await client.createSource(request)
        await refreshAll()
    }

    func pauseSource(_ name: String, reason: String = "") async throws {
        _ = try await client.pauseSource(name, reason: reason)
        await refreshAll()
    }

    func resumeSource(_ name: String) async throws {
        _ = try await client.resumeSource(name)
        await refreshAll()
    }

    func archiveSource(_ name: String) async throws {
        _ = try await client.archiveSource(name)
        await refreshAll()
    }

    func previewSourceUpgrade(_ name: String, version: Int) async throws
        -> SourceUpgradePreviewWire {
        try await client.previewSourceUpgrade(name, version: version)
    }

    func upgradeSource(
        _ name: String,
        version: Int,
        confirmationToken: String
    ) async throws {
        _ = try await client.upgradeSource(
            name,
            version: version,
            confirmationToken: confirmationToken
        )
        await refreshAll()
    }

    func previewSourceReset(_ name: String) async throws
        -> Components.Schemas.ResetPreviewResponse {
        try await client.sourceResetPreview(name)
    }

    func resetSource(_ name: String, confirmationToken: String) async throws {
        _ = try await client.resetSource(name, confirmationToken: confirmationToken)
        await refreshAll()
    }

    func createTrigger(
        source: String,
        flow: String,
        type: String,
        spec: [String: JSONValue]
    ) async throws {
        _ = try await client.createSourceTrigger(
            source,
            flow: flow,
            type: type,
            spec: spec
        )
        await refreshAll()
    }

    func setTriggerEnabled(source: String, id: Int, enabled: Bool) async throws {
        _ = try await client.patchSourceTrigger(source, id: id, enabled: enabled)
        await refreshAll()
    }

    func deleteTrigger(source: String, id: Int) async throws {
        _ = try await client.deleteSourceTrigger(source, id: id)
        await refreshAll()
    }

    func ingest(
        _ documents: [IngestDocument],
        source: String,
        mode: String,
        idempotencyKey: String
    ) async throws {
        _ = try await client.ingest(
            documents,
            into: source,
            mode: mode,
            idempotencyKey: idempotencyKey
        )
        await refreshAll()
    }

    func runPipeline(name: String, version: Int, flow: String? = nil) async throws {
        _ = try await client.runPipeline(name, version: version, flow: flow)
        await refreshAll()
    }

    func archivePipeline(_ name: String) async throws {
        _ = try await client.archivePipeline(name)
        await refreshAll()
    }

    func loadRunDetail(_ id: Int) async {
        do {
            async let detail = client.run(id, includeSpec: true)
            async let events = client.runEvents(id, limit: 1_000)
            async let outputs = client.runOutputs(id)
            let values = try await (detail, events, outputs)
            runs.applyDetail(
                values.0,
                events: try values.1.events.map { try $0.operationalEvent() },
                outputs: values.2.outputs
            )
        } catch {
            runs.failDetail(id, error: error)
        }
    }

    func artifact(runID: Int, artifactID: String) async throws -> Data {
        try await client.runArtifact(runID, artifactID: artifactID)
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
            ingress: ingress,
            configuration: configuration, status: status
        )
    }
}
