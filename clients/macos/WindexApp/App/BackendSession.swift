import Foundation
import Observation
import WindexKit
import WindexUI

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

struct SourceModuleUnavailableError: LocalizedError, Equatable, Sendable {
    let source: String
    let pipelineVersion: Int
    let latestPipelineVersion: Int
    let unavailableModules: [String]

    var errorDescription: String? {
        let modules = unavailableModules.isEmpty
            ? "one or more frozen Modules"
            : unavailableModules.joined(separator: ", ")
        return "Pipeline upgrade required for \(source). "
            + "Pipeline v\(pipelineVersion) cannot use \(modules); "
            + "preview and confirm an upgrade to v\(latestPipelineVersion)."
    }
}

private struct PipelineLayoutFlowMismatch: LocalizedError, Sendable {
    let requested: String
    let received: String

    var errorDescription: String? {
        "Layout response for \(requested) identified Flow \(received)."
    }
}

private struct PipelineRevisionIdentityMismatch: LocalizedError, Sendable {
    let expectedPipeline: String
    let expectedVersion: Int
    let receivedPipeline: String
    let receivedVersion: Int

    var errorDescription: String? {
        "Revision response identity \(receivedPipeline) v\(receivedVersion) "
            + "did not match requested \(expectedPipeline) v\(expectedVersion)."
    }
}

private struct DuplicatePipelineRevision: LocalizedError, Sendable {
    let pipeline: String
    let version: Int

    var errorDescription: String? {
        "Revision list for \(pipeline) contained duplicate v\(version) entries."
    }
}

private struct PipelineSummaryIdentityMismatch: LocalizedError, Sendable {
    let expected: String
    let received: String

    var errorDescription: String? {
        "Pipeline response identity \(received) did not match requested \(expected)."
    }
}

private struct DuplicatePipelineSummary: LocalizedError, Sendable {
    let pipeline: String

    var errorDescription: String? {
        "Pipeline catalogue contained duplicate \(pipeline) summaries."
    }
}

private struct PipelineHeadRevisionMissing: LocalizedError, Sendable {
    let pipeline: String
    let version: Int

    var errorDescription: String? {
        "Revision list for \(pipeline) omitted head v\(version)."
    }
}

private struct UnknownPipelineLayoutScope: LocalizedError, Sendable {
    let pipeline: String
    let version: Int
    let flow: String

    var errorDescription: String? {
        "Cannot load layout for unknown \(pipeline) v\(version)/\(flow)."
    }
}

/// A Source refresh is assembled from several independently served
/// projections. Keep their failures attached to the affected Source instead of
/// collapsing the complete Source catalogue into a failed state.
struct SourceLoadDiagnostic: Equatable, Sendable {
    enum Projection: String, CaseIterable, Hashable, Sendable {
        case detail
        case history
        case settings
        case triggers
        case status

        var title: String { rawValue.capitalized }
    }

    let source: String
    let failures: [Projection: String]
    let usingLastKnownGood: Bool
    let snapshotAvailable: Bool

    var message: String {
        let details = Projection.allCases.compactMap { projection in
            failures[projection].map { "\(projection.title): \($0)" }
        }.joined(separator: " ")
        let disposition: String
        if usingLastKnownGood {
            disposition = "Showing the last complete Source snapshot."
        } else if snapshotAvailable {
            disposition = "The remaining Source data is still available."
        } else {
            disposition = "The Source will appear after a complete snapshot loads."
        }
        return "\(details) \(disposition) Retry the refresh."
    }
}

/// Pipeline data is assembled from an independently served revision list and
/// one layout per Flow. Keep failures attached to their exact scope so one bad
/// revision cannot make every later Pipeline disappear.
struct PipelineLoadDiagnostic: Equatable, Sendable {
    enum Scope: Hashable, Sendable {
        case summary
        case revisions
        case revision(Int)
        case layout(version: Int, flow: String)

        var title: String {
            switch self {
            case .summary:
                "Summary"
            case .revisions:
                "Revisions"
            case .revision(let version):
                "Revision v\(version)"
            case .layout(let version, let flow):
                "Layout v\(version)/\(flow)"
            }
        }

        func belongs(to version: Int) -> Bool {
            switch self {
            case .revision(let candidate):
                candidate == version
            case .layout(let candidate, _):
                candidate == version
            case .summary, .revisions:
                false
            }
        }

        var sortKey: String {
            switch self {
            case .summary:
                "0"
            case .revisions:
                "1"
            case .revision(let version):
                "2:\(String(format: "%010d", version))"
            case .layout(let version, let flow):
                "3:\(String(format: "%010d", version)):\(flow)"
            }
        }
    }

    let pipeline: String
    let failures: [Scope: String]
    let usingLastKnownGood: Bool
    let snapshotAvailable: Bool

    var message: String {
        let details = failures.sorted {
            $0.key.sortKey < $1.key.sortKey
        }.map { scope, failure in
            "\(scope.title): \(failure)"
        }.joined(separator: " ")
        let disposition: String
        if usingLastKnownGood {
            disposition = "Showing the last complete data for the affected scope."
        } else if snapshotAvailable {
            disposition = "The remaining Pipeline data is still available."
        } else {
            disposition = "The Pipeline will be usable after a complete snapshot loads."
        }
        return "\(details) \(disposition) Retry the refresh."
    }
}

@MainActor
@Observable
final class RegistryStore {
    private(set) var registry: PipelineRegistry?
    private(set) var state: StoreLoadState = .idle
    private(set) var isStale = false
    private let cache: RegistryCache

    init(client: WindexClient) { cache = RegistryCache(client: client) }

    #if DEBUG
    func replaceForUITesting(_ value: PipelineRegistry) {
        registry = value
        isStale = false
        state = .loaded
    }
    #endif

    func load(
        when shouldApply: @escaping @MainActor () -> Bool = { true }
    ) async {
        if registry == nil, let cached = await cache.cached() {
            guard shouldApply() else { return }
            registry = try? cached.pipelineRegistry()
            isStale = registry != nil
        }
        guard shouldApply() else { return }
        state = .loading
        do {
            let loaded = try await cache.load().pipelineRegistry()
            let stale = await cache.wasStale
            guard shouldApply() else { return }
            registry = loaded
            isStale = stale
            state = .loaded
        } catch {
            guard shouldApply() else { return }
            state = .failed(error.localizedDescription)
        }
    }
}

@MainActor
@Observable
final class PipelineStore {
    struct Snapshot {
        let summary: PipelineSummary
        let revisions: [PipelineRevision]
        let layouts: [PipelineFlowLayout]
        /// Nil means no revision list has ever established its membership.
        /// A concrete set lets completeness be recomputed after targeted or
        /// per-Flow recovery instead of becoming permanently sticky-false.
        let expectedVersions: Set<Int>?

        init(
            summary: PipelineSummary,
            revisions: [PipelineRevision],
            layouts: [PipelineFlowLayout],
            expectedVersions: Set<Int>? = nil,
            revisionListKnown: Bool = true
        ) {
            self.summary = summary
            self.revisions = revisions
            self.layouts = layouts
            self.expectedVersions = revisionListKnown
                ? expectedVersions ?? Set(revisions.map(\.reference.version))
                : nil
        }

        var isComplete: Bool {
            guard let expectedVersions else { return false }
            let represented = Set(revisions.map(\.reference.version))
            return represented == expectedVersions
                && revisions.allSatisfy(hasCompleteLayouts)
        }

        func hasCompleteLayouts(for revision: PipelineRevision) -> Bool {
            let expected = Set(revision.spec.flows.map(\.name))
            let available = Set(layouts.lazy.filter {
                $0.pipeline == revision.reference.pipeline
                    && $0.version == revision.reference.version
            }.map(\.flow))
            return expected.isSubset(of: available)
        }
    }

    private(set) var pipelines: [PipelineSummary] = []
    private(set) var revisions: [String: [PipelineRevision]] = [:]
    private(set) var layouts: [String: PipelineFlowLayout] = [:]
    private(set) var loadDiagnostics: [String: PipelineLoadDiagnostic] = [:]
    private(set) var state: StoreLoadState = .idle
    private var expectedRevisionVersions: [String: Set<Int>] = [:]
    private var knownRevisionLists: Set<String> = []

    func loading() { state = .loading }
    func fail(_ error: Error) { state = .failed(error.localizedDescription) }
    func replace(_ values: [PipelineSummary]) {
        pipelines = values.sorted {
            $0.displayTitle.localizedStandardCompare($1.displayTitle) == .orderedAscending
        }
        state = .loaded
    }
    /// Atomically install complete per-Pipeline snapshots. Callers stage
    /// revisions and all Flow layouts first, preventing a new revision from
    /// being paired with a partial layout refresh.
    func replaceSnapshots(
        _ values: [Snapshot],
        diagnostics: [String: PipelineLoadDiagnostic]
    ) {
        pipelines = values.map(\.summary).sorted {
            $0.displayTitle.localizedStandardCompare($1.displayTitle)
                == .orderedAscending
        }
        revisions = Dictionary(uniqueKeysWithValues: values.map { snapshot in
            (
                snapshot.summary.name,
                snapshot.revisions.sorted {
                    $0.reference.version > $1.reference.version
                }
            )
        })
        layouts = Dictionary(
            uniqueKeysWithValues: values.flatMap(\.layouts).map { layout in
                (
                    Self.layoutKey(
                        pipeline: layout.pipeline,
                        version: layout.version,
                        flow: layout.flow
                    ),
                    layout
                )
            }
        )
        loadDiagnostics = diagnostics
        knownRevisionLists = Set(values.compactMap {
            $0.expectedVersions == nil ? nil : $0.summary.name
        })
        expectedRevisionVersions = Dictionary(
            uniqueKeysWithValues: values.compactMap { snapshot in
                snapshot.expectedVersions.map {
                    (snapshot.summary.name, $0)
                }
            }
        )
        state = .loaded
    }
    func snapshot(for pipeline: String) -> Snapshot? {
        guard let summary = pipelines.first(where: { $0.name == pipeline }) else {
            return nil
        }
        return Snapshot(
            summary: summary,
            revisions: revisions[pipeline] ?? [],
            layouts: layouts.values.filter { $0.pipeline == pipeline },
            expectedVersions: expectedRevisionVersions[pipeline],
            revisionListKnown: knownRevisionLists.contains(pipeline)
        )
    }
    func apply(_ pipeline: PipelineSummary) {
        if let index = pipelines.firstIndex(where: { $0.name == pipeline.name }) {
            pipelines[index] = pipeline
        } else {
            pipelines.append(pipeline)
        }
        pipelines.sort {
            $0.displayTitle.localizedStandardCompare($1.displayTitle) == .orderedAscending
        }
        state = .loaded
    }
    func reconcileDeploymentCounts(_ deployments: [SourceDeployment]) {
        let counts = Dictionary(grouping: deployments, by: {
            $0.pipeline.pipeline
        }).mapValues(\.count)
        pipelines = pipelines.map { pipeline in
            PipelineSummary(
                name: pipeline.name,
                title: pipeline.title,
                description: pipeline.description,
                headVersion: pipeline.headVersion,
                headHash: pipeline.headHash,
                builtin: pipeline.builtin,
                archived: pipeline.archived,
                deploymentCount: counts[pipeline.name] ?? 0
            )
        }
    }
    func replaceRevisions(_ values: [PipelineRevision], for pipeline: String) {
        revisions[pipeline] = values.sorted { $0.reference.version > $1.reference.version }
        knownRevisionLists.insert(pipeline)
        expectedRevisionVersions[pipeline] = Set(
            values.map(\.reference.version)
        )
    }
    func apply(_ revision: PipelineRevision) {
        var values = revisions[revision.reference.pipeline] ?? []
        if let index = values.firstIndex(where: {
            $0.reference.version == revision.reference.version
        }) {
            values[index] = revision
        } else {
            values.append(revision)
        }
        revisions[revision.reference.pipeline] = values.sorted {
            $0.reference.version > $1.reference.version
        }
        expectRevision(
            revision.reference.version,
            for: revision.reference.pipeline
        )
    }
    /// Apply one targeted immutable revision and its complete layout set.
    /// Existing historical revisions remain untouched.
    func applySnapshot(
        summary: PipelineSummary,
        revision: PipelineRevision?,
        layouts nextLayouts: [PipelineFlowLayout]
    ) {
        apply(summary)
        guard let revision else {
            return
        }
        apply(revision)
        let pipeline = revision.reference.pipeline
        let version = revision.reference.version
        layouts = layouts.filter { _, layout in
            layout.pipeline != pipeline || layout.version != version
        }
        for layout in nextLayouts {
            apply(layout)
        }
    }
    func apply(_ layout: PipelineFlowLayout) {
        layouts[Self.layoutKey(pipeline: layout.pipeline, version: layout.version,
                                flow: layout.flow)] = layout
    }
    func layout(pipeline: String, version: Int, flow: String) -> PipelineFlowLayout? {
        layouts[Self.layoutKey(pipeline: pipeline, version: version, flow: flow)]
    }
    func revision(pipeline: String, version: Int) -> PipelineRevision? {
        revisions[pipeline]?.first {
            $0.reference.version == version
        }
    }
    func expectRevision(_ version: Int, for pipeline: String) {
        expectedRevisionVersions[pipeline, default: []].insert(version)
    }
    func hasCompleteLayouts(for revision: PipelineRevision) -> Bool {
        snapshot(for: revision.reference.pipeline)?
            .hasCompleteLayouts(for: revision) == true
    }
    func diagnostic(for pipeline: String) -> PipelineLoadDiagnostic? {
        loadDiagnostics[pipeline]
    }
    func recordFailure(
        for pipeline: String,
        scope: PipelineLoadDiagnostic.Scope,
        error: Error,
        usingLastKnownGood: Bool,
        snapshotAvailable: Bool
    ) {
        var failures = loadDiagnostics[pipeline]?.failures ?? [:]
        failures[scope] = error.localizedDescription
        loadDiagnostics[pipeline] = .init(
            pipeline: pipeline,
            failures: failures,
            usingLastKnownGood: loadDiagnostics[pipeline]?.usingLastKnownGood == true
                || usingLastKnownGood,
            snapshotAvailable: loadDiagnostics[pipeline]?.snapshotAvailable == true
                || snapshotAvailable
        )
        state = .loaded
    }
    func replaceRevisionFailures(
        for pipeline: String,
        version: Int,
        failures nextFailures: [PipelineLoadDiagnostic.Scope: String],
        usingLastKnownGood: Bool,
        snapshotAvailable: Bool
    ) {
        let current = loadDiagnostics[pipeline]
        var failures = current?.failures.filter {
            !$0.key.belongs(to: version)
        } ?? [:]
        let retainedFailure = !failures.isEmpty
        failures.merge(nextFailures) { _, next in next }
        guard !failures.isEmpty else {
            loadDiagnostics.removeValue(forKey: pipeline)
            state = .loaded
            return
        }
        loadDiagnostics[pipeline] = .init(
            pipeline: pipeline,
            failures: failures,
            usingLastKnownGood: usingLastKnownGood
                || (retainedFailure && current?.usingLastKnownGood == true),
            snapshotAvailable: snapshotAvailable
                || (retainedFailure && current?.snapshotAvailable == true)
        )
        state = .loaded
    }
    func resolveFailure(
        for pipeline: String,
        scope: PipelineLoadDiagnostic.Scope
    ) {
        guard let diagnostic = loadDiagnostics[pipeline] else { return }
        var failures = diagnostic.failures
        failures.removeValue(forKey: scope)
        guard !failures.isEmpty else {
            loadDiagnostics.removeValue(forKey: pipeline)
            return
        }
        loadDiagnostics[pipeline] = .init(
            pipeline: pipeline,
            failures: failures,
            usingLastKnownGood: diagnostic.usingLastKnownGood,
            snapshotAvailable: diagnostic.snapshotAvailable
        )
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
    private(set) var loadDiagnostics: [String: SourceLoadDiagnostic] = [:]
    private(set) var configuredSecrets: [String] = []
    private(set) var moduleHealth: ModuleHealthWire?
    private(set) var moduleStatuses: [String: SourceModuleStatusWire] = [:]
    private(set) var moduleDiagnosticsState: StoreLoadState = .idle
    private(set) var state: StoreLoadState = .idle
    func loading() { state = .loading }
    func fail(_ error: Error) { state = .failed(error.localizedDescription) }
    func replace(_ values: [SourceDeployment]) {
        sources = values.sorted {
            $0.displayTitle.localizedStandardCompare($1.displayTitle) == .orderedAscending
        }
        state = .loaded
    }
    /// Atomically installs the per-Source projections assembled by a complete
    /// refresh. Callers preserve the previous values for any Source whose new
    /// projection could not be assembled, so an ETag or trigger set can never
    /// move ahead of its deployment/configuration snapshot.
    func replaceSnapshots(
        _ values: [SourceDeployment],
        settingsETags: [String: String],
        triggers: [String: [SourceTriggerWire]],
        diagnostics: [String: SourceLoadDiagnostic]
    ) {
        sources = values.sorted {
            $0.displayTitle.localizedStandardCompare($1.displayTitle) == .orderedAscending
        }
        self.settingsETags = settingsETags
        self.triggers = triggers.mapValues { Self.sortedTriggers($0) }
        loadDiagnostics = diagnostics
        state = .loaded
    }
    func applySnapshot(
        _ source: SourceDeployment,
        settingsETag: String,
        triggers: [SourceTriggerWire],
        diagnostic: SourceLoadDiagnostic?
    ) {
        apply(source)
        settingsETags[source.name] = settingsETag
        self.triggers[source.name] = Self.sortedTriggers(triggers)
        loadDiagnostics[source.name] = diagnostic
        state = .loaded
    }
    func setLoadDiagnostic(_ diagnostic: SourceLoadDiagnostic) {
        loadDiagnostics[diagnostic.source] = diagnostic
    }
    func resolveLoadDiagnostic(
        for source: String,
        projection: SourceLoadDiagnostic.Projection
    ) {
        guard let current = loadDiagnostics[source] else { return }
        var failures = current.failures
        failures.removeValue(forKey: projection)
        guard !failures.isEmpty else {
            loadDiagnostics.removeValue(forKey: source)
            return
        }
        loadDiagnostics[source] = .init(
            source: source,
            failures: failures,
            usingLastKnownGood: current.usingLastKnownGood,
            snapshotAvailable: current.snapshotAvailable
        )
    }
    func diagnostic(for source: String) -> SourceLoadDiagnostic? {
        loadDiagnostics[source]
    }
    func setSettingsETag(_ etag: String, for source: String) {
        settingsETags[source] = etag
    }
    func setTriggers(_ values: [SourceTriggerWire], for source: String) {
        triggers[source] = Self.sortedTriggers(values)
    }
    func setConfiguredSecrets(_ values: [String]) {
        configuredSecrets = values.sorted()
    }
    func loadingModuleDiagnostics() {
        moduleDiagnosticsState = .loading
    }
    func failModuleDiagnostics(_ error: Error) {
        moduleDiagnosticsState = .failed(error.localizedDescription)
    }
    func replaceModuleDiagnostics(
        health: ModuleHealthWire,
        statuses: [SourceModuleStatusWire]
    ) {
        moduleHealth = health
        moduleStatuses = Dictionary(
            uniqueKeysWithValues: statuses.map { ($0.source, $0) }
        )
        moduleDiagnosticsState = .loaded
    }
    func applyModuleDiagnostics(
        health: ModuleHealthWire,
        status: SourceModuleStatusWire
    ) {
        moduleHealth = health
        moduleStatuses[status.source] = status
        moduleDiagnosticsState = .loaded
    }
    func moduleStatus(for source: String) -> SourceModuleStatusWire? {
        moduleStatuses[source]
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
    private static func sortedTriggers(
        _ values: [SourceTriggerWire]
    ) -> [SourceTriggerWire] {
        values.sorted {
            ($0.nextFireAt ?? "\u{10ffff}") < ($1.nextFireAt ?? "\u{10ffff}")
        }
    }
}

/// One connection-scoped configuration editor per Source. Both Source detail
/// and global Settings ask this store for the same FormModel instance, so
/// navigation and background reconciliation cannot discard an unsaved edit.
@MainActor
@Observable
final class SourceSettingsDraftStore {
    private var forms: [String: FormModel] = [:]

    func form(for source: String) -> FormModel? {
        forms[source]
    }

    @discardableResult
    func reconcile(_ scope: SettingsScope) -> FormModel {
        if let current = forms[scope.scope] {
            guard !current.isDirty else { return current }
            if current.params == scope.fields.map(\.param) {
                current.apply(scope.fields)
                return current
            }
        }
        let form = FormModel(scope: scope)
        forms[scope.scope] = form
        return form
    }

    @discardableResult
    func adopt(_ scope: SettingsScope) -> FormModel {
        if let current = forms[scope.scope],
           current.params == scope.fields.map(\.param) {
            current.apply(scope.fields)
            return current
        }
        let form = FormModel(scope: scope)
        forms[scope.scope] = form
        return form
    }

    func removeMissingSources(_ names: Set<String>) {
        forms = forms.filter { names.contains($0.key) }
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
    private(set) var sourceHistoryHasMore: [String: Bool] = [:]
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
    func runs(for source: String) -> [SourceRunSummary] {
        runs.filter { $0.sourceName == source }
    }
    func mergeSourceHistory(
        _ values: [SourceRunSummary],
        source: String,
        replacing: Bool
    ) {
        if replacing {
            runs.removeAll { $0.sourceName == source }
        }
        for run in values {
            if let index = runs.firstIndex(where: { $0.id == run.id }) {
                runs[index] = run
            } else {
                runs.append(run)
            }
        }
        runs.sort { $0.id > $1.id }
        sourceHistoryHasMore[source] = values.count == 200
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
    private static let presetsKey = "windex.console-filter-presets.v2"
    private var buffer = OperationalEventBuffer()
    var filter = OperationalEventFilter()
    var followsNewest = true
    var selectedSequence: Int64?
    var presets: [OperationalEventFilterPreset] = []
    private(set) var facets = OperationalEventFacets()
    private(set) var historyState: StoreLoadState = .idle
    private(set) var historyCursor = 0
    private(set) var historyHasMore = true
    private(set) var connection: LiveConnectionState = .idle
    var events: [OperationalEvent] { buffer.values.filter(filter.includes) }
    var allEvents: [OperationalEvent] { buffer.values }
    var newestCursor: Int64? { buffer.newestCursor }
    var selectedEvent: OperationalEvent? {
        guard let selectedSequence else { return nil }
        return buffer.values.first { $0.sequence == selectedSequence }
    }
    init() {
        if let data = UserDefaults.standard.data(forKey: Self.presetsKey),
           let values = try? JSONDecoder().decode(
            [OperationalEventFilterPreset].self,
            from: data
           ) {
            presets = values
        }
    }
    func append(_ values: [OperationalEvent]) { buffer.append(values) }
    func clearLocalView() { buffer.clear(); selectedSequence = nil }
    func setConnection(_ value: LiveConnectionState) { connection = value }
    func setFacets(_ value: OperationalEventFacets) { facets = value }
    func loadingHistory(reset: Bool) {
        if reset {
            historyCursor = 0
            historyHasMore = true
        }
        historyState = .loading
    }
    func loadedHistory(nextCursor: Int, count: Int) {
        historyHasMore = count > 0 && nextCursor > historyCursor
        historyCursor = max(historyCursor, nextCursor)
        historyState = .loaded
    }
    func failedHistory(_ error: Error) {
        historyState = .failed(error.localizedDescription)
    }
    func savePreset(named rawName: String) {
        let name = rawName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else { return }
        if let index = presets.firstIndex(where: {
            $0.name.localizedCaseInsensitiveCompare(name) == .orderedSame
        }) {
            presets[index].filter = filter
        } else {
            presets.append(.init(name: name, filter: filter))
        }
        persistPresets()
    }
    func deletePreset(_ id: UUID) {
        presets.removeAll { $0.id == id }
        persistPresets()
    }
    func applyPreset(_ id: UUID) {
        guard let preset = presets.first(where: { $0.id == id }) else { return }
        filter = preset.filter
    }
    private func persistPresets() {
        guard let data = try? JSONEncoder().encode(presets) else { return }
        UserDefaults.standard.set(data, forKey: Self.presetsKey)
    }
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

/// The smallest set of canonical projections invalidated by a burst of
/// control-plane events. Sets make reconciliation cost depend on the unique
/// affected resources, rather than on the number of journal rows in the burst.
struct ControlReconciliationPlan: Equatable, Sendable {
    struct PipelineTarget: Hashable, Sendable {
        let name: String
        let version: Int?
    }

    var refreshAll = false
    var refreshRegistry = false
    var refreshModuleDiagnostics = false
    var refreshOverview = false
    var pipelines: Set<PipelineTarget> = []
    var sourceDetails: Set<String> = []
    var sourceStatuses: Set<String> = []
    var sourceTriggers: Set<String> = []
    var runs: Set<Int> = []

    static let full = Self(refreshAll: true)

    var isEmpty: Bool {
        !refreshAll
            && !refreshRegistry
            && !refreshModuleDiagnostics
            && !refreshOverview
            && pipelines.isEmpty
            && sourceDetails.isEmpty
            && sourceStatuses.isEmpty
            && sourceTriggers.isEmpty
            && runs.isEmpty
    }

    init(
        refreshAll: Bool = false,
        refreshRegistry: Bool = false,
        refreshModuleDiagnostics: Bool = false,
        refreshOverview: Bool = false,
        pipelines: Set<PipelineTarget> = [],
        sourceDetails: Set<String> = [],
        sourceStatuses: Set<String> = [],
        sourceTriggers: Set<String> = [],
        runs: Set<Int> = []
    ) {
        self.refreshAll = refreshAll
        self.refreshRegistry = refreshRegistry
        self.refreshModuleDiagnostics = refreshModuleDiagnostics
        self.refreshOverview = refreshOverview
        self.pipelines = pipelines
        self.sourceDetails = sourceDetails
        self.sourceStatuses = sourceStatuses
        self.sourceTriggers = sourceTriggers
        self.runs = runs
    }

    init(event: OperationalEvent) {
        self.init(refreshOverview: true)
        var recognizedScope = false

        if let runID = event.runID {
            runs.insert(runID)
            recognizedScope = true
        }
        if let source = event.sourceName {
            sourceStatuses.insert(source)
        }

        if event.component == "pipeline" || event.event.hasPrefix("pipeline.") {
            if let pipeline = event.pipelineName {
                pipelines.insert(.init(
                    name: pipeline,
                    version: event.pipelineVersion
                ))
                recognizedScope = true
            } else {
                refreshAll = true
            }
        }
        if event.component == "source" || event.event.hasPrefix("source.") {
            if let source = event.sourceName {
                sourceDetails.insert(source)
                recognizedScope = true
            } else {
                refreshAll = true
            }
        }
        if event.component == "scheduler" || event.event.hasPrefix("trigger.") {
            if let source = event.sourceName {
                sourceTriggers.insert(source)
                recognizedScope = true
            } else {
                refreshAll = true
            }
        }
        if event.component == "module_admin" || event.event.hasPrefix("module.") {
            refreshRegistry = true
            refreshModuleDiagnostics = true
            recognizedScope = true
        }
        if event.component == "maintenance"
            || event.event.hasPrefix("storage.gc.") {
            recognizedScope = true
        }

        // A scheduled Run changes last-fired/next-fire state in the same
        // transaction as run.queued, without a separate trigger event.
        if let trigger = event.data["trigger"]?.stringValue,
           trigger == "schedule" || trigger == "event",
           let source = event.sourceName {
            sourceTriggers.insert(source)
        }

        // Unknown global event domains must not be acknowledged by refreshing
        // Overview alone: a newer backend may be invalidating a store this
        // client does not yet know how to classify.
        if !recognizedScope {
            refreshAll = true
        }
    }

    mutating func formUnion(_ other: Self) {
        refreshAll = refreshAll || other.refreshAll
        refreshRegistry = refreshRegistry || other.refreshRegistry
        refreshModuleDiagnostics =
            refreshModuleDiagnostics || other.refreshModuleDiagnostics
        refreshOverview = refreshOverview || other.refreshOverview
        pipelines.formUnion(other.pipelines)
        sourceDetails.formUnion(other.sourceDetails)
        sourceStatuses.formUnion(other.sourceStatuses)
        sourceTriggers.formUnion(other.sourceTriggers)
        runs.formUnion(other.runs)
    }
}

private struct LoadedSourceProjection {
    let deployment: SourceDeployment
    let settingsETag: String
    let settingsScope: SettingsScope
    let triggers: [SourceTriggerWire]
    let history: [SourceRunSummary]?
}

private struct SourceProjectionAttempt {
    let projection: LoadedSourceProjection?
    let failures: [SourceLoadDiagnostic.Projection: String]
}

private struct PipelineSnapshotAttempt {
    let snapshot: PipelineStore.Snapshot
    let failures: [PipelineLoadDiagnostic.Scope: String]
    let usingLastKnownGood: Bool
}

/// Connection-scoped owner of the authoritative epoch-2 projections.
@MainActor
@Observable
final class BackendSession {
    let draftRecovery = PipelineDraftRecoveryStore()
    let registry: RegistryStore
    let pipelines = PipelineStore()
    let sources = SourceStore()
    let sourceSettingsDrafts = SourceSettingsDraftStore()
    let runs = SharedRunStore()
    let overview = SharedOverviewStore()
    let logs = SharedLogStore()
    let events = LiveEventHub()
    let client: WindexClient
    let backend: ConnectedBackend

    private(set) var hasStarted = false
    private var controlTask: Task<Void, Never>?
    private var logTask: Task<Void, Never>?
    private var reconciliationTask: Task<Void, Never>?
    private var pollingTask: Task<Void, Never>?
    private var pendingReconciliation = ControlReconciliationPlan()
    private var reconciliationRevision: UInt64 = 0
    private var completedReconciliationRevision: UInt64 = 0
    private var reconciliationWaiters:
        [(revision: UInt64, continuation: CheckedContinuation<Void, Never>)] = []
    private var lifecycleEpoch: UInt64 = 0
    private let reconciliationDelay: Duration

    init(
        client: WindexClient,
        backend: ConnectedBackend,
        reconciliationDelay: Duration = .milliseconds(250)
    ) {
        self.client = client
        self.backend = backend
        self.reconciliationDelay = reconciliationDelay
        registry = RegistryStore(client: client)
    }

    func start() async {
        guard !hasStarted else { return }
        let epoch = lifecycleEpoch
        hasStarted = true
        await refreshAll()
        guard hasStarted, lifecycleEpoch == epoch else { return }
        startStreams()
    }

    func foreground() async {
        guard hasStarted else { await start(); return }
        let epoch = lifecycleEpoch
        await refreshAll()
        guard hasStarted, lifecycleEpoch == epoch else { return }
        if controlTask == nil || logTask == nil { startStreams() }
    }

    func refreshAll() async {
        let revision = enqueueReconciliation(.full)
        await waitForReconciliation(revision)
    }

    func stop() {
        lifecycleEpoch &+= 1
        controlTask?.cancel()
        logTask?.cancel()
        reconciliationTask?.cancel()
        pollingTask?.cancel()
        controlTask = nil
        logTask = nil
        reconciliationTask = nil
        pollingTask = nil
        pendingReconciliation = .init()
        completedReconciliationRevision = reconciliationRevision
        resumeReconciliationWaiters()
        events.stop()
        logs.setConnection(.idle)
        hasStarted = false
    }

    private func performFullRefresh(epoch: UInt64) async {
        await registry.load(when: { [weak self] in
            self?.reconciliationIsCurrent(epoch) == true
        })
        guard reconciliationIsCurrent(epoch) else { return }
        await loadRuns(epoch: epoch)
        guard reconciliationIsCurrent(epoch) else { return }
        await loadSources(epoch: epoch)
        guard reconciliationIsCurrent(epoch) else { return }
        await loadPipelines(epoch: epoch)
        guard reconciliationIsCurrent(epoch) else { return }
        await loadModuleDiagnostics(epoch: epoch)
        guard reconciliationIsCurrent(epoch) else { return }
        await loadLogs(epoch: epoch)
        guard reconciliationIsCurrent(epoch) else { return }
        await loadOverview(epoch: epoch)
    }

    private func loadPipelines(epoch: UInt64) async {
        guard reconciliationIsCurrent(epoch) else { return }
        pipelines.loading()
        do {
            let response = try await client.pipelines()
            guard reconciliationIsCurrent(epoch) else { return }
            let deploymentCounts = Dictionary(grouping: sources.sources,
                                              by: { $0.pipeline.pipeline })
                .mapValues(\.count)
            let groupedWires = Dictionary(
                grouping: response.pipelines,
                by: \.name
            )
            let summaries = groupedWires.values.compactMap { wires in
                wires.first.map {
                    $0.summary(
                        deploymentCount: deploymentCounts[$0.name] ?? 0
                    )
                }
            }
            let previous = Dictionary(
                uniqueKeysWithValues: summaries.compactMap { summary in
                    pipelines.snapshot(for: summary.name).map {
                        (summary.name, $0)
                    }
                }
            )
            let previousDiagnostics = pipelines.loadDiagnostics
            var snapshots: [PipelineStore.Snapshot] = []
            var diagnostics: [String: PipelineLoadDiagnostic] = [:]
            for summary in summaries {
                if groupedWires[summary.name, default: []].count > 1 {
                    let retained = previous[summary.name]
                    let error = DuplicatePipelineSummary(
                        pipeline: summary.name
                    )
                    var failures =
                        previousDiagnostics[summary.name]?.failures ?? [:]
                    failures[.summary] = error.localizedDescription
                    let snapshot = retained ?? .init(
                        summary: summary,
                        revisions: [],
                        layouts: [],
                        revisionListKnown: false
                    )
                    snapshots.append(snapshot)
                    diagnostics[summary.name] = .init(
                        pipeline: summary.name,
                        failures: failures,
                        usingLastKnownGood: retained?.isComplete == true,
                        snapshotAvailable: !snapshot.revisions.isEmpty
                    )
                    continue
                }
                guard let attempt = await loadPipelineSnapshot(
                    summary,
                    previous: previous[summary.name],
                    previousDiagnostic: previousDiagnostics[summary.name],
                    epoch: epoch
                ) else {
                    return
                }
                snapshots.append(attempt.snapshot)
                if !attempt.failures.isEmpty {
                    diagnostics[summary.name] = .init(
                        pipeline: summary.name,
                        failures: attempt.failures,
                        usingLastKnownGood: attempt.usingLastKnownGood,
                        snapshotAvailable: !attempt.snapshot.revisions.isEmpty
                    )
                }
            }
            guard reconciliationIsCurrent(epoch) else { return }
            pipelines.replaceSnapshots(snapshots, diagnostics: diagnostics)
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            pipelines.fail(error)
        }
    }

    /// Assemble one Pipeline without mutating the shared store. Each revision
    /// and its complete Flow-layout set are staged together; a failure retains
    /// the exact previous revision projection when one exists.
    private func loadPipelineSnapshot(
        _ summary: PipelineSummary,
        previous: PipelineStore.Snapshot?,
        previousDiagnostic: PipelineLoadDiagnostic?,
        epoch: UInt64
    ) async -> PipelineSnapshotAttempt? {
        let response: PipelineRevisionsWire
        do {
            response = try await client.pipelineRevisions(summary.name)
        } catch {
            guard reconciliationIsCurrent(epoch) else { return nil }
            var failures = previousDiagnostic?.failures.filter {
                $0.key != .summary
            } ?? [:]
            failures[.revisions] = error.localizedDescription
            return .init(
                snapshot: previous ?? .init(
                    summary: summary,
                    revisions: [],
                    layouts: [],
                    revisionListKnown: false
                ),
                failures: failures,
                usingLastKnownGood: previous?.isComplete == true
            )
        }
        guard reconciliationIsCurrent(epoch) else { return nil }

        let previousRevisions = Dictionary(
            uniqueKeysWithValues: (previous?.revisions ?? []).map {
                ($0.reference.version, $0)
            }
        )
        var revisions: [PipelineRevision] = []
        var layouts: [PipelineFlowLayout] = []
        var failures: [PipelineLoadDiagnostic.Scope: String] = [:]
        var usingLastKnownGood = false
        let versionCounts = Dictionary(
            grouping: response.revisions,
            by: \.version
        ).mapValues(\.count)
        var handledDuplicateVersions: Set<Int> = []

        for wire in response.revisions {
            let version = wire.version
            if versionCounts[version, default: 0] > 1 {
                guard handledDuplicateVersions.insert(version).inserted else {
                    continue
                }
                let error = DuplicatePipelineRevision(
                    pipeline: summary.name,
                    version: version
                )
                failures[.revision(version)] = error.localizedDescription
                if let retained = previousRevisions[version],
                   previous?.hasCompleteLayouts(for: retained) == true {
                    revisions.append(retained)
                    layouts.append(contentsOf: previous?.layouts.filter {
                        $0.version == version
                    } ?? [])
                    usingLastKnownGood = true
                }
                continue
            }
            let revision: PipelineRevision
            do {
                revision = try wire.revision(
                    title: summary.title,
                    description: summary.description
                )
                guard revision.reference.pipeline == summary.name,
                      revision.reference.version == version else {
                    throw PipelineRevisionIdentityMismatch(
                        expectedPipeline: summary.name,
                        expectedVersion: version,
                        receivedPipeline: revision.reference.pipeline,
                        receivedVersion: revision.reference.version
                    )
                }
            } catch {
                failures[.revision(version)] = error.localizedDescription
                if let retained = previousRevisions[version],
                   previous?.hasCompleteLayouts(for: retained) == true {
                    revisions.append(retained)
                    layouts.append(contentsOf: previous?.layouts.filter {
                        $0.version == version
                    } ?? [])
                    usingLastKnownGood = true
                }
                continue
            }

            guard let layoutAttempt = await loadPipelineLayouts(
                revision,
                pipeline: summary.name,
                version: version,
                epoch: epoch
            ) else {
                return nil
            }
            if layoutAttempt.failures.isEmpty {
                revisions.append(revision)
                layouts.append(contentsOf: layoutAttempt.layouts)
            } else if let retained = previousRevisions[version],
                      previous?.hasCompleteLayouts(for: retained) == true {
                failures.merge(layoutAttempt.failures) { _, next in next }
                // Do not mix successfully refreshed Flow layouts with stale
                // siblings. Retain the complete exact-revision projection.
                revisions.append(retained)
                layouts.append(contentsOf: previous?.layouts.filter {
                    $0.version == version
                } ?? [])
                usingLastKnownGood = true
            } else {
                // The semantic revision remains useful (and immutable), but no
                // partial layout set is published into the shared cache.
                revisions.append(revision)
                failures.merge(
                    completeLayoutFailures(
                        for: revision,
                        actual: layoutAttempt.failures
                    )
                ) { _, next in next }
            }
        }

        var expectedVersions = Set(response.revisions.map(\.version))
        if summary.headVersion > 0,
           !expectedVersions.contains(summary.headVersion) {
            expectedVersions.insert(summary.headVersion)
            let error = PipelineHeadRevisionMissing(
                pipeline: summary.name,
                version: summary.headVersion
            )
            failures[.revisions] = error.localizedDescription
            if let retained = previousRevisions[summary.headVersion],
               previous?.hasCompleteLayouts(for: retained) == true {
                revisions.append(retained)
                layouts.append(contentsOf: previous?.layouts.filter {
                    $0.version == summary.headVersion
                } ?? [])
                usingLastKnownGood = true
            }
        }

        return .init(
            snapshot: .init(
                summary: summary,
                revisions: revisions,
                layouts: layouts,
                expectedVersions: expectedVersions
            ),
            failures: failures,
            usingLastKnownGood: usingLastKnownGood
        )
    }

    private func loadPipelineLayouts(
        _ revision: PipelineRevision,
        pipeline: String,
        version: Int,
        epoch: UInt64
    ) async -> (
        layouts: [PipelineFlowLayout],
        failures: [PipelineLoadDiagnostic.Scope: String]
    )? {
        var layouts: [PipelineFlowLayout] = []
        var failures: [PipelineLoadDiagnostic.Scope: String] = [:]
        for flow in revision.spec.flows {
            do {
                let wire = try await client.pipelineLayout(
                    pipeline,
                    version: version,
                    flow: flow.name
                )
                guard reconciliationIsCurrent(epoch) else { return nil }
                let layout = try wire.flowLayout(
                    pipeline: pipeline,
                    version: version
                )
                guard layout.flow == flow.name else {
                    throw PipelineLayoutFlowMismatch(
                        requested: flow.name,
                        received: layout.flow
                    )
                }
                layouts.append(layout)
            } catch {
                guard reconciliationIsCurrent(epoch) else { return nil }
                failures[.layout(
                    version: version,
                    flow: flow.name
                )] = error.localizedDescription
            }
        }
        return (layouts, failures)
    }

    /// When a staged layout set is discarded, every absent Flow remains
    /// diagnosable. A later one-Flow load therefore cannot make an incomplete
    /// revision look healthy.
    private func completeLayoutFailures(
        for revision: PipelineRevision,
        actual: [PipelineLoadDiagnostic.Scope: String]
    ) -> [PipelineLoadDiagnostic.Scope: String] {
        var failures = actual
        for flow in revision.spec.flows {
            let scope = PipelineLoadDiagnostic.Scope.layout(
                version: revision.reference.version,
                flow: flow.name
            )
            if failures[scope] == nil {
                failures[scope] =
                    "Withheld because another Flow layout failed to refresh."
            }
        }
        return failures
    }

    /// Refresh one Pipeline head and, when the event identifies it, one exact
    /// immutable revision. This deliberately never walks historical revisions.
    private func loadPipeline(
        _ target: ControlReconciliationPlan.PipelineTarget,
        epoch: UInt64
    ) async {
        let previous = pipelines.snapshot(for: target.name)
        let wire: PipelineWire
        do {
            wire = try await client.pipeline(target.name)
            guard reconciliationIsCurrent(epoch) else { return }
            guard wire.name == target.name else {
                throw PipelineSummaryIdentityMismatch(
                    expected: target.name,
                    received: wire.name
                )
            }
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            pipelines.recordFailure(
                for: target.name,
                scope: .summary,
                error: error,
                usingLastKnownGood: previous?.isComplete == true,
                snapshotAvailable: previous?.revisions.isEmpty == false
            )
            return
        }
        pipelines.resolveFailure(for: target.name, scope: .summary)

        let deploymentCount = sources.sources.lazy.filter {
            $0.pipeline.pipeline == target.name
        }.count
        let summary = wire.summary(deploymentCount: deploymentCount)
        guard let version = target.version ?? wire.version else {
            pipelines.applySnapshot(
                summary: summary,
                revision: nil,
                layouts: []
            )
            return
        }

        let retainedRevision = previous?.revisions.first {
            $0.reference.version == version
        }
        let retainedIsComplete = retainedRevision.map {
            previous?.hasCompleteLayouts(for: $0) == true
        } ?? false
        let revision: PipelineRevision
        do {
            let revisionWire = try await client.pipelineRevision(
                target.name,
                version: version
            )
            guard reconciliationIsCurrent(epoch) else { return }
            revision = try revisionWire.revision(
                title: summary.title,
                description: summary.description
            )
            guard revision.reference.pipeline == target.name,
                  revision.reference.version == version else {
                throw PipelineRevisionIdentityMismatch(
                    expectedPipeline: target.name,
                    expectedVersion: version,
                    receivedPipeline: revision.reference.pipeline,
                    receivedVersion: revision.reference.version
                )
            }
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            if previous == nil {
                pipelines.applySnapshot(
                    summary: summary,
                    revision: nil,
                    layouts: []
                )
            }
            pipelines.expectRevision(version, for: target.name)
            pipelines.replaceRevisionFailures(
                for: target.name,
                version: version,
                failures: [.revision(version): error.localizedDescription],
                usingLastKnownGood: retainedIsComplete,
                snapshotAvailable: previous?.revisions.isEmpty == false
            )
            return
        }

        guard let layoutAttempt = await loadPipelineLayouts(
            revision,
            pipeline: target.name,
            version: version,
            epoch: epoch
        ) else {
            return
        }
        let nextFailures: [PipelineLoadDiagnostic.Scope: String]
        if layoutAttempt.failures.isEmpty {
            pipelines.applySnapshot(
                summary: summary,
                revision: revision,
                layouts: layoutAttempt.layouts
            )
            nextFailures = [:]
        } else if retainedIsComplete {
            nextFailures = layoutAttempt.failures
        } else {
            pipelines.applySnapshot(
                summary: summary,
                revision: revision,
                layouts: []
            )
            nextFailures = completeLayoutFailures(
                for: revision,
                actual: layoutAttempt.failures
            )
        }
        pipelines.replaceRevisionFailures(
            for: target.name,
            version: version,
            failures: nextFailures,
            usingLastKnownGood: retainedIsComplete,
            snapshotAvailable: retainedRevision != nil
                || previous?.revisions.isEmpty == false
        )
    }

    private func loadRuns(epoch: UInt64) async {
        guard reconciliationIsCurrent(epoch) else { return }
        runs.loading()
        do {
            let response = try await client.runs(limit: 200)
            guard reconciliationIsCurrent(epoch) else { return }
            runs.replace(try response.runs.map {
                try $0.summary()
            })
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            runs.fail(error)
        }
    }

    private func loadRun(_ id: Int, epoch: UInt64) async {
        if runs.details[id] != nil {
            await loadRunDetail(id, epoch: epoch)
            return
        }
        do {
            let response = try await client.run(id)
            guard reconciliationIsCurrent(epoch) else { return }
            runs.apply(try response.summary())
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            runs.fail(error)
        }
    }

    private func loadSources(epoch: UInt64) async {
        guard reconciliationIsCurrent(epoch) else { return }
        sources.loading()
        do {
            let response = try await client.sources()
            guard reconciliationIsCurrent(epoch) else { return }
            let configuredSecrets = try? await client.secrets()
            guard reconciliationIsCurrent(epoch) else { return }

            let listedNames = Set(response.sources.map(\.name))
            let previousDeployments = Dictionary(
                uniqueKeysWithValues: sources.sources.map { ($0.name, $0) }
            )
            let previousETags = sources.settingsETags
            let previousTriggers = sources.triggers
            var deployments: [SourceDeployment] = []
            var settingsETags: [String: String] = [:]
            var triggers: [String: [SourceTriggerWire]] = [:]
            var diagnostics: [String: SourceLoadDiagnostic] = [:]
            var loaded: [LoadedSourceProjection] = []

            for wire in response.sources {
                guard let attempt = await loadSourceProjection(
                    from: wire,
                    refreshDetail: true,
                    epoch: epoch
                ) else {
                    return
                }
                let previous = previousDeployments[wire.name]
                if let projection = attempt.projection {
                    deployments.append(projection.deployment)
                    settingsETags[wire.name] = projection.settingsETag
                    triggers[wire.name] = projection.triggers
                    loaded.append(projection)
                } else if let previous {
                    // Keep the whole last-known-good projection. Never advance
                    // its ETag or triggers independently of the deployment.
                    deployments.append(previous)
                    if let etag = previousETags[wire.name] {
                        settingsETags[wire.name] = etag
                    }
                    if let values = previousTriggers[wire.name] {
                        triggers[wire.name] = values
                    }
                }
                if !attempt.failures.isEmpty {
                    diagnostics[wire.name] = .init(
                        source: wire.name,
                        failures: attempt.failures,
                        usingLastKnownGood:
                            attempt.projection == nil && previous != nil,
                        snapshotAvailable:
                            attempt.projection != nil || previous != nil
                    )
                }
            }
            guard reconciliationIsCurrent(epoch) else { return }

            // These mutations deliberately contain no suspension point. The UI
            // observes either the prior snapshot or this complete transaction,
            // never new ancillary state paired with an old deployment.
            for projection in loaded {
                if let history = projection.history {
                    runs.mergeSourceHistory(
                        history,
                        source: projection.deployment.name,
                        replacing: true
                    )
                }
                sourceSettingsDrafts.reconcile(projection.settingsScope)
            }
            sourceSettingsDrafts.removeMissingSources(listedNames)
            sources.replaceSnapshots(
                deployments,
                settingsETags: settingsETags,
                triggers: triggers,
                diagnostics: diagnostics
            )
            if let configuredSecrets {
                sources.setConfiguredSecrets(
                    configuredSecrets.secrets.filter(\.configured).map(\.name)
                )
            }
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            sources.fail(error)
        }
    }

    /// Fetches and decodes every projection needed to update one Source, but
    /// performs no store mutation. Settings, triggers, and status form the
    /// consistency boundary. Detail and history can fall back independently,
    /// with their failures retained as Source-specific diagnostics.
    private func loadSourceProjection(
        from summary: SourceWire,
        refreshDetail: Bool,
        epoch: UInt64
    ) async -> SourceProjectionAttempt? {
        var failures: [SourceLoadDiagnostic.Projection: String] = [:]
        var detail = summary
        if refreshDetail {
            do {
                detail = try await client.source(summary.name)
            } catch {
                failures[.detail] = error.localizedDescription
            }
            guard reconciliationIsCurrent(epoch) else { return nil }
        }

        var history: [SourceRunSummary]?
        do {
            let response = try await client.sourceRuns(summary.name, limit: 200)
            history = try response.runs.map { try $0.summary() }
        } catch {
            failures[.history] = error.localizedDescription
        }
        guard reconciliationIsCurrent(epoch) else { return nil }

        var settings:
            (etag: String, scope: SettingsScope, effective: [String: JSONValue])?
        do {
            let response = try await client.sourceSettings(summary.name)
            settings = (
                response.etag,
                try response.settingsScope(),
                try response.values.additionalProperties.decode(
                    [String: JSONValue].self
                )
            )
        } catch {
            failures[.settings] = error.localizedDescription
        }
        guard reconciliationIsCurrent(epoch) else { return nil }

        var triggerValues: [SourceTriggerWire]?
        do {
            let response = try await client.sourceTriggers(summary.name)
            triggerValues = response.triggers
        } catch {
            failures[.triggers] = error.localizedDescription
        }
        guard reconciliationIsCurrent(epoch) else { return nil }

        var statusWire: SourceStatusWire?
        do {
            statusWire = try await client.sourceStatus(summary.name)
        } catch {
            failures[.status] = error.localizedDescription
        }
        guard reconciliationIsCurrent(epoch) else { return nil }

        guard let settings, let triggerValues, let statusWire else {
            return .init(projection: nil, failures: failures)
        }

        let nextTrigger = triggerValues
            .filter(\.enabled)
            .compactMap(\.nextFireAt)
            .sorted()
            .first
        var statusRuns = runs.runs
        if let history {
            statusRuns.removeAll { $0.sourceName == summary.name }
            statusRuns.append(contentsOf: history)
        }
        let status: SourceStatus
        do {
            status = try statusWire.status(
                runs: statusRuns,
                nextTrigger: nextTrigger
            )
        } catch {
            failures[.status] = error.localizedDescription
            return .init(projection: nil, failures: failures)
        }

        let base: SourceDeployment
        do {
            base = try detail.deployment(status: status)
        } catch {
            failures[.detail] = error.localizedDescription
            return .init(projection: nil, failures: failures)
        }
        let deployment = base.withConfiguration(.init(
            fields: settings.scope.fields.map(\.param),
            configuredValues: base.configuration.configuredValues,
            effectiveValues: settings.effective,
            origins: Dictionary(
                uniqueKeysWithValues: settings.scope.fields.compactMap { field in
                    field.origin.map { origin in
                        (field.key, origin.rawValue)
                    }
                }
            ),
            missingRequired: settings.scope.fields.compactMap {
                $0.param.required && settings.effective[$0.key] == nil
                    ? $0.key
                    : nil
            },
            valuesHash: settings.etag
        ))
        return .init(
            projection: .init(
                deployment: deployment,
                settingsETag: settings.etag,
                settingsScope: settings.scope,
                triggers: triggerValues,
                history: history
            ),
            failures: failures
        )
    }

    private func loadSourceTriggers(_ name: String, epoch: UInt64) async {
        do {
            let response = try await client.sourceTriggers(name)
            guard reconciliationIsCurrent(epoch) else { return }
            sources.setTriggers(response.triggers, for: name)
            if let current = sources.sources.first(where: {
                $0.name == name
            }) {
                let nextTrigger = response.triggers
                    .filter(\.enabled)
                    .compactMap(\.nextFireAt)
                    .sorted()
                    .first
                sources.apply(current.withStatus(
                    current.status.withNextTrigger(nextTrigger)
                ))
            }
            sources.resolveLoadDiagnostic(for: name, projection: .triggers)
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            recordSourceLoadFailure(
                name,
                projection: .triggers,
                error: error
            )
        }
    }

    /// Refresh every projection belonging to one Source. Unlike loadSources(),
    /// this cost is bounded to the named deployment and its 200-Run history.
    /// Lifecycle events use it because revision and settings may change
    /// together, and because the Source may not have existed in this session.
    @discardableResult
    private func loadSource(
        _ name: String,
        epoch: UInt64
    ) async -> SourceDeployment? {
        let detail: SourceWire
        do {
            detail = try await client.source(name)
            guard reconciliationIsCurrent(epoch) else { return nil }
        } catch {
            guard reconciliationIsCurrent(epoch) else { return nil }
            recordSourceLoadFailure(
                name,
                projection: .detail,
                error: error
            )
            return sources.sources.first { $0.name == name }
        }

        guard let attempt = await loadSourceProjection(
            from: detail,
            refreshDetail: false,
            epoch: epoch
        ) else {
            return nil
        }
        guard let projection = attempt.projection else {
            applySourceLoadDiagnostic(
                name: name,
                failures: attempt.failures,
                usingLastKnownGood: sources.sources.contains {
                    $0.name == name
                }
            )
            return sources.sources.first { $0.name == name }
        }
        guard reconciliationIsCurrent(epoch) else { return nil }

        if let history = projection.history {
            runs.mergeSourceHistory(
                history,
                source: name,
                replacing: true
            )
        }
        sourceSettingsDrafts.reconcile(projection.settingsScope)
        let diagnostic = sourceLoadDiagnostic(
            name: name,
            failures: attempt.failures,
            usingLastKnownGood: false,
            snapshotAvailable: true
        )
        sources.applySnapshot(
            projection.deployment,
            settingsETag: projection.settingsETag,
            triggers: projection.triggers,
            diagnostic: diagnostic
        )
        do {
            async let healthRequest = client.moduleHealth()
            async let moduleStatusRequest = client.sourceModuleStatus(name)
            let diagnostics = try await (
                healthRequest,
                moduleStatusRequest
            )
            guard reconciliationIsCurrent(epoch) else { return nil }
            sources.applyModuleDiagnostics(
                health: diagnostics.0,
                status: diagnostics.1
            )
        } catch {
            guard reconciliationIsCurrent(epoch) else { return nil }
            // Runtime status/configuration remains useful when the diagnostic
            // projection is temporarily unavailable.
            sources.failModuleDiagnostics(error)
        }
        pipelines.reconcileDeploymentCounts(sources.sources)
        if !pipelines.pipelines.contains(where: {
            $0.name == projection.deployment.pipeline.pipeline
        }) {
            await loadPipeline(.init(
                name: projection.deployment.pipeline.pipeline,
                version: projection.deployment.pipeline.version
            ), epoch: epoch)
        }
        return projection.deployment
    }

    /// Run/Task events only invalidate runtime status. If the Source is new to
    /// this session, promote the refresh to the complete single-Source path.
    private func loadSourceStatus(_ name: String, epoch: UInt64) async {
        guard sources.sources.contains(where: { $0.name == name }) else {
            await loadSource(name, epoch: epoch)
            return
        }
        do {
            let wire = try await client.sourceStatus(name)
            guard reconciliationIsCurrent(epoch) else { return }
            let nextTrigger = sources.triggers[name]?
                .filter(\.enabled)
                .compactMap(\.nextFireAt)
                .sorted()
                .first
            guard let current = sources.sources.first(where: {
                $0.name == name
            }) else {
                await loadSource(name, epoch: epoch)
                return
            }
            sources.apply(current.withStatus(try wire.status(
                runs: runs.runs,
                nextTrigger: nextTrigger
            )))
            sources.resolveLoadDiagnostic(for: name, projection: .status)
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            recordSourceLoadFailure(
                name,
                projection: .status,
                error: error
            )
        }
    }

    private func recordSourceLoadFailure(
        _ name: String,
        projection: SourceLoadDiagnostic.Projection,
        error: Error
    ) {
        var failures = sources.diagnostic(for: name)?.failures ?? [:]
        failures[projection] = error.localizedDescription
        applySourceLoadDiagnostic(
            name: name,
            failures: failures,
            usingLastKnownGood: sources.sources.contains { $0.name == name }
        )
    }

    private func applySourceLoadDiagnostic(
        name: String,
        failures: [SourceLoadDiagnostic.Projection: String],
        usingLastKnownGood: Bool
    ) {
        guard let diagnostic = sourceLoadDiagnostic(
            name: name,
            failures: failures,
            usingLastKnownGood: usingLastKnownGood,
            snapshotAvailable: sources.sources.contains { $0.name == name }
        ) else { return }
        sources.setLoadDiagnostic(diagnostic)
    }

    private func sourceLoadDiagnostic(
        name: String,
        failures: [SourceLoadDiagnostic.Projection: String],
        usingLastKnownGood: Bool,
        snapshotAvailable: Bool
    ) -> SourceLoadDiagnostic? {
        guard !failures.isEmpty else { return nil }
        return .init(
            source: name,
            failures: failures,
            usingLastKnownGood: usingLastKnownGood,
            snapshotAvailable: snapshotAvailable
        )
    }

    private func loadLogs(epoch: UInt64) async {
        do {
            async let eventResponse = client.logEvents(
                .init(after: logs.newestCursor.flatMap(Int.init), limit: 500)
            )
            async let facetResponse = client.logFacets()
            let (response, facets) = try await (eventResponse, facetResponse)
            guard reconciliationIsCurrent(epoch) else { return }
            logs.append(try response.events.map { try $0.operationalEvent() })
            logs.setFacets(facets.facets())
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            logs.setConnection(.degraded(error.localizedDescription))
        }
    }

    private func loadModuleDiagnostics(epoch: UInt64) async {
        guard reconciliationIsCurrent(epoch) else { return }
        sources.loadingModuleDiagnostics()
        do {
            let health = try await client.moduleHealth()
            guard reconciliationIsCurrent(epoch) else { return }
            var statuses: [SourceModuleStatusWire] = []
            for source in sources.sources {
                statuses.append(
                    try await client.sourceModuleStatus(source.name)
                )
                guard reconciliationIsCurrent(epoch) else { return }
            }
            sources.replaceModuleDiagnostics(
                health: health,
                statuses: statuses
            )
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            sources.failModuleDiagnostics(error)
        }
    }

    func loadLogHistory(continuing: Bool = false) async {
        logs.loadingHistory(reset: !continuing)
        let after = continuing ? logs.historyCursor : 0
        do {
            let response = try await client.logEvents(
                .init(filter: logs.filter, after: after, limit: 500)
            )
            let events = try response.events.map { try $0.operationalEvent() }
            logs.append(events)
            logs.loadedHistory(
                nextCursor: response.nextCursor,
                count: events.count
            )
        } catch {
            logs.failedHistory(error)
        }
    }

    private func loadOverview(epoch: UInt64) async {
        guard reconciliationIsCurrent(epoch) else { return }
        overview.loading()
        do {
            let response = try await client.overview()
            guard reconciliationIsCurrent(epoch) else { return }
            let failures = logs.allEvents.filter {
                $0.level == .error || $0.level == .critical
            }.suffix(20)
            overview.apply(try response.snapshot(
                sourceDeployments: sources.sources,
                recentFailures: Array(failures)
            ))
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
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
                    if let wire = try? event.decode(OperationalEventWire.self),
                       let decoded = try? wire.operationalEvent() {
                        scheduleReconciliation(for: decoded)
                    } else {
                        // A forward contract can add event fields before this
                        // client knows how to scope them. Never acknowledge
                        // that invalidation with only a partial projection.
                        scheduleReconciliation(.full)
                    }
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

    func scheduleReconciliation(for event: OperationalEvent) {
        scheduleReconciliation(.init(event: event))
    }

    private func scheduleReconciliation(_ plan: ControlReconciliationPlan) {
        _ = enqueueReconciliation(plan)
    }

    /// One driver owns every projection refresh. Work queued before a pass is
    /// coalesced; work queued after its snapshot remains pending for the next
    /// pass. Revisions let callers await their invalidation without creating a
    /// second refresh task, while the lifecycle epoch rejects late responses
    /// from a session that has been stopped.
    @discardableResult
    private func enqueueReconciliation(
        _ plan: ControlReconciliationPlan
    ) -> UInt64 {
        guard !plan.isEmpty else { return reconciliationRevision }
        pendingReconciliation.formUnion(plan)
        reconciliationRevision &+= 1
        let revision = reconciliationRevision
        guard reconciliationTask == nil else { return revision }
        let epoch = lifecycleEpoch
        reconciliationTask = Task { [weak self] in
            guard let self else { return }
            try? await Task.sleep(for: self.reconciliationDelay)
            guard self.reconciliationIsCurrent(epoch) else {
                self.finishReconciliation(epoch: epoch)
                return
            }
            await self.drainReconciliation(epoch: epoch)
        }
        return revision
    }

    private func drainReconciliation(epoch: UInt64) async {
        while reconciliationIsCurrent(epoch), !pendingReconciliation.isEmpty {
            let plan = pendingReconciliation
            let revision = reconciliationRevision
            pendingReconciliation = .init()
            await reconcile(plan, epoch: epoch)
            guard reconciliationIsCurrent(epoch) else { break }
            completedReconciliationRevision = max(
                completedReconciliationRevision,
                revision
            )
            resumeReconciliationWaiters()
        }
        finishReconciliation(epoch: epoch)
    }

    private func finishReconciliation(epoch: UInt64) {
        guard lifecycleEpoch == epoch else { return }
        reconciliationTask = nil
        resumeReconciliationWaiters()
    }

    private func reconciliationIsCurrent(_ epoch: UInt64) -> Bool {
        lifecycleEpoch == epoch && !Task.isCancelled
    }

    private func waitForReconciliation(_ revision: UInt64) async {
        guard completedReconciliationRevision < revision else { return }
        await withCheckedContinuation { continuation in
            reconciliationWaiters.append((revision, continuation))
        }
    }

    private func resumeReconciliationWaiters() {
        var remaining:
            [(revision: UInt64, continuation: CheckedContinuation<Void, Never>)] = []
        for waiter in reconciliationWaiters {
            if waiter.revision <= completedReconciliationRevision {
                waiter.continuation.resume()
            } else {
                remaining.append(waiter)
            }
        }
        reconciliationWaiters = remaining
    }

    /// Await all work queued when this method is called. Kept internal so
    /// deterministic app-model tests can assert request cardinality without
    /// timing guesses.
    func waitForScheduledReconciliation() async {
        await waitForReconciliation(reconciliationRevision)
    }

    private func reconcile(
        _ plan: ControlReconciliationPlan,
        epoch: UInt64
    ) async {
        if plan.refreshAll {
            await performFullRefresh(epoch: epoch)
            return
        }
        if plan.refreshRegistry {
            await registry.load(when: { [weak self] in
                self?.reconciliationIsCurrent(epoch) == true
            })
        }
        guard reconciliationIsCurrent(epoch) else { return }
        for runID in plan.runs.sorted() {
            await loadRun(runID, epoch: epoch)
        }
        for source in plan.sourceDetails.sorted() {
            await loadSource(source, epoch: epoch)
        }
        for source in plan.sourceTriggers
            .subtracting(plan.sourceDetails)
            .sorted() {
            await loadSourceTriggers(source, epoch: epoch)
        }
        for source in plan.sourceStatuses
            .subtracting(plan.sourceDetails)
            .sorted() {
            await loadSourceStatus(source, epoch: epoch)
        }
        for target in plan.pipelines.sorted(by: {
            if $0.name == $1.name {
                return ($0.version ?? -1) < ($1.version ?? -1)
            }
            return $0.name < $1.name
        }) {
            await loadPipeline(target, epoch: epoch)
        }
        if plan.refreshModuleDiagnostics {
            await loadModuleDiagnostics(epoch: epoch)
        }
        if plan.refreshOverview {
            await loadOverview(epoch: epoch)
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
        try await saveLayouts([layout])
    }

    func saveLayouts(_ layouts: [PipelineFlowLayout]) async throws {
        for layout in layouts {
            _ = try await client.putPipelineLayout(layout)
        }
        await refreshAll()
    }

    func loadLayout(pipeline: String, version: Int, flow: String) async {
        let epoch = lifecycleEpoch
        guard reconciliationIsCurrent(epoch) else { return }
        guard let revision = pipelines.revision(
            pipeline: pipeline,
            version: version
        ), revision.spec.flows.contains(where: { $0.name == flow }) else {
            pipelines.recordFailure(
                for: pipeline,
                scope: .layout(version: version, flow: flow),
                error: UnknownPipelineLayoutScope(
                    pipeline: pipeline,
                    version: version,
                    flow: flow
                ),
                usingLastKnownGood: false,
                snapshotAvailable: pipelines.snapshot(for: pipeline) != nil
            )
            return
        }
        do {
            let wire = try await client.pipelineLayout(
                pipeline,
                version: version,
                flow: flow
            )
            guard reconciliationIsCurrent(epoch) else { return }
            let layout = try wire.flowLayout(
                pipeline: pipeline,
                version: version
            )
            guard layout.flow == flow else {
                throw PipelineLayoutFlowMismatch(
                    requested: flow,
                    received: layout.flow
                )
            }
            guard reconciliationIsCurrent(epoch) else { return }
            pipelines.apply(layout)
            pipelines.resolveFailure(
                for: pipeline,
                scope: .layout(version: version, flow: flow)
            )
        } catch {
            guard reconciliationIsCurrent(epoch) else { return }
            pipelines.recordFailure(
                for: pipeline,
                scope: .layout(version: version, flow: flow),
                error: error,
                usingLastKnownGood: pipelines.layout(
                    pipeline: pipeline,
                    version: version,
                    flow: flow
                ) != nil,
                snapshotAvailable: pipelines.snapshot(for: pipeline) != nil
            )
        }
    }

    func saveSourceSettings(_ name: String, values: [String: JSONValue]) async throws {
        guard let etag = sources.settingsETags[name] else {
            throw WindexError.preconditionRequired(message: "Source settings ETag is unavailable.")
        }
        let response = try await client.patchSourceSettings(
            name,
            values: values,
            etag: etag
        )
        sources.setSettingsETag(response.etag, for: name)
        sourceSettingsDrafts.adopt(try response.settingsScope())
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

    func validateSource(_ request: SourceCreateRequest) async throws -> SourceValidationWire {
        try await client.validateSource(request)
    }

    func setSourceEnabled(_ name: String, enabled: Bool) async throws {
        _ = try await client.patchSource(name, enabled: enabled)
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

    func previewSourceUpgrade(
        _ name: String,
        version: Int,
        values: [String: JSONValue]? = nil
    ) async throws
        -> SourceUpgradePreviewWire {
        try await client.previewSourceUpgrade(
            name,
            version: version,
            values: values
        )
    }

    func upgradeSource(
        _ name: String,
        version: Int,
        values: [String: JSONValue],
        confirmationToken: String
    ) async throws {
        _ = try await client.upgradeSource(
            name,
            version: version,
            values: values,
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
        spec: [String: JSONValue],
        enabled: Bool = true
    ) async throws {
        _ = try await client.createSourceTrigger(
            source,
            flow: flow,
            type: type,
            enabled: enabled,
            spec: spec
        )
        await refreshAll()
    }

    func updateTrigger(
        source: String,
        id: Int,
        flow: String,
        type: String,
        spec: [String: JSONValue],
        enabled: Bool
    ) async throws {
        _ = try await client.patchSourceTrigger(
            source,
            id: id,
            flow: flow,
            type: type,
            enabled: enabled,
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
        partition: String? = nil,
        idempotencyKey: String
    ) async throws {
        try requireSourceAvailable(source)
        _ = try await client.ingest(
            documents,
            into: source,
            mode: mode,
            partition: partition,
            idempotencyKey: idempotencyKey
        )
        await refreshAll()
    }

    func runPipeline(
        name: String,
        version: Int,
        flow: String? = nil,
        dryRun: Bool = false
    ) async throws {
        _ = try await client.runPipeline(
            name,
            version: version,
            flow: flow,
            dryRun: dryRun
        )
        await refreshAll()
    }

    func archivePipeline(_ name: String) async throws {
        _ = try await client.archivePipeline(name)
        await refreshAll()
    }

    func loadRunDetail(_ id: Int) async {
        await loadRunDetail(id, epoch: nil)
    }

    private func loadRunDetail(_ id: Int, epoch: UInt64?) async {
        do {
            async let detail = client.run(id, includeSpec: true)
            async let events = client.runEvents(id, limit: 1_000)
            async let outputs = client.runOutputs(id)
            let values = try await (detail, events, outputs)
            if let epoch {
                guard reconciliationIsCurrent(epoch) else { return }
            }
            runs.apply(try values.0.summary())
            runs.applyDetail(
                values.0,
                events: try values.1.events.map { try $0.operationalEvent() },
                outputs: values.2.outputs
            )
        } catch {
            if let epoch {
                guard reconciliationIsCurrent(epoch) else { return }
            }
            runs.failDetail(id, error: error)
        }
    }

    func loadMoreSourceRuns(_ source: String) async {
        guard let beforeID = runs.runs(for: source).last?.id else { return }
        do {
            let response = try await client.sourceRuns(
                source,
                beforeID: beforeID,
                limit: 200
            )
            runs.mergeSourceHistory(
                try response.runs.map { try $0.summary() },
                source: source,
                replacing: false
            )
        } catch {
            runs.fail(error)
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
        try requireSourceAvailable(source)
        _ = try await client.runLatestSource(source)
        await refreshAll()
    }

    private func requireSourceAvailable(_ name: String) throws {
        guard let status = sources.moduleStatus(for: name),
              !status.available else { return }
        throw SourceModuleUnavailableError(
            source: name,
            pipelineVersion: status.pipelineVersion,
            latestPipelineVersion: status.latestPipelineVersion,
            unavailableModules: status.unavailableModules
        )
    }
}

private extension SourceDeployment {
    func withConfiguration(_ configuration: SourceConfiguration) -> SourceDeployment {
        SourceDeployment(
            name: name, title: title, description: description, origin: origin,
            originValues: originValues,
            pipeline: pipeline, search: search, stateNamespace: stateNamespace,
            enabled: enabled, paused: paused, archived: archived, generation: generation,
            ingress: ingress,
            configuration: configuration, status: status
        )
    }

    func withStatus(_ status: SourceStatus) -> SourceDeployment {
        SourceDeployment(
            name: name, title: title, description: description, origin: origin,
            originValues: originValues,
            pipeline: pipeline, search: search, stateNamespace: stateNamespace,
            enabled: enabled, paused: paused, archived: archived, generation: generation,
            ingress: ingress,
            configuration: configuration, status: status
        )
    }
}

private extension SourceStatus {
    func withNextTrigger(_ nextTrigger: String?) -> SourceStatus {
        SourceStatus(
            activity: activity,
            counts: counts,
            currentRun: currentRun,
            latestRun: latestRun,
            nextTrigger: nextTrigger,
            recentError: recentError
        )
    }
}
