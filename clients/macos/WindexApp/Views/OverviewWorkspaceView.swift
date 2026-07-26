import SwiftUI
import WindexKit
import WindexUI

struct OverviewView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend

    @Environment(BackendSession.self) private var session
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                runningHead
                Hairline()

                if let snapshot = session.overview.snapshot {
                    headline(snapshot)
                    Hairline()
                    moduleLockHealth(snapshot)
                    Hairline()
                    runPressure(snapshot)
                    if !snapshot.workerLanes.isEmpty {
                        Hairline()
                        workerPressure(snapshot)
                    }
                    Hairline()
                    sourceTable(snapshot.sources)
                    if !snapshot.activeRuns.isEmpty || !snapshot.recentRuns.isEmpty {
                        Hairline()
                        runTable(snapshot)
                    }
                    if !snapshot.recentDocuments.isEmpty {
                        Hairline()
                        recentDocuments(snapshot.recentDocuments)
                    }
                    Hairline()
                    serviceTable(snapshot.services)
                    if !snapshot.recentFailures.isEmpty {
                        Hairline()
                        recentFailures(snapshot.recentFailures)
                    }
                } else {
                    waitingState
                }
            }
            .padding(.xl)
            .frame(maxWidth: 1120, alignment: .leading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(theme.palette.ink)
    }

    private var runningHead: some View {
        HStack(alignment: .firstTextBaseline) {
            StyledText("Windex", Typography.masthead)
            Spacer()
            connectionStatus
            Text(backend.profile.displayAddress)
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
        }
    }

    @ViewBuilder
    private var connectionStatus: some View {
        switch session.events.connection {
        case .live:
            StatusBadge(.running, word: "live")
        case .connecting:
            StatusBadge(.running, word: "connecting")
        case .degraded:
            StatusBadge(.attention, word: "REST fallback")
        case .idle:
            StatusBadge(.attention, word: "offline")
        }
    }

    private func headline(_ snapshot: OverviewSnapshot) -> some View {
        VStack(alignment: .leading, spacing: .lg) {
            HStack(alignment: .firstTextBaseline, spacing: .xl) {
                setFigure(snapshot.searchable.formatted(), "searchable documents")
                setFigure(snapshot.documents.formatted(), "documents")
            }
            HStack(spacing: .xl) {
                measure("indexed last hour", snapshot.indexedLastHour)
                measure("vectors", snapshot.vectors.map(String.init) ?? "unavailable")
                measure(
                    "snapshot",
                    snapshot.generatedAt.formatted(
                        .dateTime.hour().minute().second()
                    )
                )
                measure("event revision", String(snapshot.revision))
            }
        }
    }

    private func setFigure(_ value: String, _ label: String) -> some View {
        VStack(alignment: .leading, spacing: .xxs) {
            StyledText(value, Typography.setXL)
            Text(label)
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
        }
    }

    private func measure(_ label: String, _ value: Int) -> some View {
        measure(label, value.formatted())
    }

    private func measure(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: .xxs) {
            Text(label)
                .windexStyle(Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            Text(value)
                .windexStyle(Typography.data)
        }
    }

    private func runPressure(_ snapshot: OverviewSnapshot) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Run pressure", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            HStack(spacing: .xl) {
                measure("running", snapshot.runs.running)
                measure("queued", snapshot.runs.queued)
                measure("blocked", snapshot.runs.blocked)
                measure("failed", snapshot.runs.failed)
                measure("succeeded", snapshot.runs.succeeded)
                measure("cancelled", snapshot.runs.cancelled)
            }
        }
    }

    private func moduleLockHealth(
        _ snapshot: OverviewSnapshot
    ) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            HStack {
                StyledText("Pipeline Module locks", Typography.eyebrow)
                    .foregroundStyle(theme.palette.graphite)
                Spacer()
                StatusBadge(
                    snapshot.moduleLocks.badgeStatus,
                    word: snapshot.moduleLocks.rawValue
                )
            }

            switch snapshot.moduleLocks {
            case .ok:
                Text("Every enabled Source is pinned to Modules available on this deployment.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            case .degraded:
                Text(
                    "\(snapshot.strandedSources.count) Source(s) require a Pipeline "
                        + "upgrade before they can start new work."
                )
                .windexStyle(Typography.body)
                moduleHealthRows
            case .error:
                Text(
                    "Module-lock health could not be evaluated. Inspect the Source "
                        + "diagnostics before starting new work."
                )
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.rust)
            }

            if case .failed(let message) = session.sources.moduleDiagnosticsState {
                Text(message)
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.rust)
                    .textSelection(.enabled)
            }
        }
    }

    @ViewBuilder
    private var moduleHealthRows: some View {
        if let health = session.sources.moduleHealth {
            ForEach(health.sources, id: \.source) { status in
                HStack(alignment: .firstTextBaseline, spacing: .sm) {
                    Button(status.source) {
                        appModel.openSource(status.source)
                    }
                    .buttonStyle(.plain)
                    .windexStyle(Typography.label)
                    .frame(width: 150, alignment: .leading)
                    Text(
                        "revision \(status.pipelineRevisionId) · "
                            + "v\(status.pipelineVersion) → v\(status.latestPipelineVersion)"
                    )
                    .windexStyle(Typography.dataSM)
                    .frame(width: 220, alignment: .leading)
                    Text(
                        status.unavailableModules.isEmpty
                            ? "Unavailable Modules not reported"
                            : status.unavailableModules.joined(separator: ", ")
                    )
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.rust)
                    Spacer()
                }
            }
        } else {
            ForEach(
                session.overview.snapshot?.strandedSources ?? [],
                id: \.self
            ) { source in
                Button(source) {
                    appModel.openSource(source)
                }
                .buttonStyle(.plain)
                .windexStyle(Typography.label)
            }
        }
    }

    private func workerPressure(_ snapshot: OverviewSnapshot) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Worker lanes", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            ForEach(snapshot.workerLanes) { lane in
                HStack(spacing: .lg) {
                    Text(lane.name)
                        .windexStyle(Typography.label)
                        .frame(width: 140, alignment: .leading)
                    ForEach(lane.states.keys.sorted(), id: \.self) { state in
                        Text("\(state) \(lane.states[state, default: 0])")
                            .windexStyle(Typography.dataSM)
                    }
                }
            }
            ForEach(snapshot.blockedPreconditions) { item in
                Text(
                    "\(item.tasks) blocked · \(item.preconditions.joined(separator: ", "))"
                        + (item.reason.map { " · \($0)" } ?? "")
                )
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.rust)
            }
        }
    }

    private func sourceTable(_ sources: [OverviewSourceStatus]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Sources", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)

            HStack {
                tableHeader("Source", width: 170)
                tableHeader("Pipeline", width: 170)
                tableHeader("Documents", width: 90, alignment: .trailing)
                tableHeader("Searchable", width: 90, alignment: .trailing)
                tableHeader("Next trigger", width: 180)
                tableHeader("State", width: 105)
                Spacer()
            }

            ForEach(sources) { row in
                Hairline()
                HStack {
                    Button(row.source.displayTitle) {
                        appModel.openSource(row.source.name)
                    }
                    .buttonStyle(.plain)
                        .frame(width: 170, alignment: .leading)
                    Button(
                        "\(row.source.pipeline.pipeline) @ \(row.source.pipeline.version)"
                    ) {
                        appModel.openPipeline(row.source.pipeline)
                    }
                    .buttonStyle(.plain)
                        .frame(width: 170, alignment: .leading)
                    Text(row.documents.formatted())
                        .frame(width: 90, alignment: .trailing)
                    Text(row.searchable.formatted())
                        .frame(width: 90, alignment: .trailing)
                    Text(row.nextTrigger.map(shortTimestamp) ?? "—")
                        .frame(width: 180, alignment: .leading)
                    Group {
                        if session.overview.snapshot?.strandedSources.contains(
                            row.source.name
                        ) == true {
                            StatusBadge(.fault, word: "upgrade required")
                        } else {
                            StatusBadge(
                                row.source.status.activity.overviewStatus,
                                word: row.source.status.activity.rawValue
                            )
                        }
                    }
                    .frame(width: 105, alignment: .leading)
                    Spacer()
                }
                .windexStyle(Typography.data)
                .frame(minHeight: 28)
            }
        }
    }

    private func runTable(_ snapshot: OverviewSnapshot) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Active and recent Runs", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            ForEach((snapshot.activeRuns + snapshot.recentRuns.prefix(8))) { run in
                HStack(spacing: .sm) {
                    Button("#\(run.id)") {
                        appModel.openRun(run.id)
                    }
                    .buttonStyle(.plain)
                        .windexStyle(Typography.data)
                        .frame(width: 54, alignment: .leading)
                    Text(run.sourceName ?? "generic")
                        .windexStyle(Typography.label)
                        .frame(width: 130, alignment: .leading)
                    Button(
                        "\(run.pipelineName) @ \(run.pipelineVersion) · \(run.flowName)"
                    ) {
                        appModel.openPipeline(
                            .init(
                                pipeline: run.pipelineName,
                                version: run.pipelineVersion,
                                specHash: ""
                            ),
                            flow: run.flowName
                        )
                    }
                    .buttonStyle(.plain)
                        .windexStyle(Typography.dataSM)
                        .frame(width: 260, alignment: .leading)
                    Text(run.state)
                        .windexStyle(Typography.dataSM)
                    if let progress = run.progress {
                        ProgressView(value: progress)
                            .frame(width: 100)
                    }
                    Spacer()
                }
            }
        }
    }

    private func recentDocuments(_ documents: [OverviewRecentDocument]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Recently indexed", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            ForEach(documents.prefix(8)) { document in
                HStack(spacing: .sm) {
                    Text(document.source)
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                        .frame(width: 120, alignment: .leading)
                    Text(document.title)
                        .windexStyle(Typography.body)
                        .lineLimit(1)
                    Spacer()
                    Text(shortTimestamp(document.indexedAt))
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                }
            }
        }
    }

    private func serviceTable(_ services: [OverviewServiceStatus]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Services", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            ForEach(services) { service in
                HStack {
                    Text(service.name)
                        .windexStyle(Typography.label)
                        .frame(width: 180, alignment: .leading)
                    StatusBadge(
                        service.available ? .healthy : .fault,
                        word: service.available ? "available" : "unavailable"
                    )
                    if let detail = service.detail {
                        Text(detail)
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.graphite)
                    }
                }
            }
        }
    }

    private func recentFailures(_ failures: [OperationalEvent]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Recent failures", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            ForEach(failures.prefix(8)) { event in
                Button {
                    appModel.openConsole(
                        OperationalEventFilter(
                            levels: [event.level],
                            sourceName: event.sourceName,
                            pipelineName: event.pipelineName,
                            runID: event.runID,
                            node: event.node,
                            module: event.module
                        )
                    )
                } label: {
                    HStack(alignment: .firstTextBaseline, spacing: .sm) {
                    Text(event.timestamp, format: .dateTime.hour().minute().second())
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                        .frame(width: 80, alignment: .leading)
                    StatusBadge(.fault, word: event.level.rawValue)
                    Text(event.message)
                        .windexStyle(Typography.body)
                    Spacer()
                    }
                }
                .buttonStyle(.plain)
            }
        }
    }

    private var waitingState: some View {
        VStack(alignment: .leading, spacing: .md) {
            StyledText("Loading the all-up projection", Typography.setLG)
            Text(
                "Overview reconciles the canonical corpus totals, Run pressure, worker lanes, Source schedules, recent documents, and service health."
            )
            .windexStyle(Typography.body)
            .foregroundStyle(theme.palette.graphite)
            .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
        }
        .padding(.top, .lg)
    }

    private func tableHeader(
        _ value: String,
        width: CGFloat,
        alignment: Alignment = .leading
    ) -> some View {
        Text(value)
            .windexStyle(Typography.eyebrow)
            .foregroundStyle(theme.palette.graphite)
            .frame(width: width, alignment: alignment)
    }

    private func shortTimestamp(_ value: String) -> String {
        value.replacingOccurrences(of: "T", with: " ").prefix(19).description
    }
}

private extension SourceActivityState {
    var overviewStatus: Status {
        switch self {
        case .idle, .succeeded:
            .healthy
        case .queued, .running:
            .running
        case .blocked, .paused:
            .attention
        case .failed, .cancelled, .archived:
            .fault
        }
    }
}

private extension OverviewModuleLockHealth {
    var badgeStatus: Status {
        switch self {
        case .ok:
            .healthy
        case .degraded:
            .attention
        case .error:
            .fault
        }
    }
}
