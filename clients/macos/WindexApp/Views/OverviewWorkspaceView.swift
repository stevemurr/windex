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
                    runPressure(snapshot.runs)
                    Hairline()
                    sourceTable(snapshot.sources)
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
            StatusBadge(.attention, word: "degraded")
        case .idle, .awaitingContract:
            StatusBadge(.attention, word: "awaiting canonical stream")
        }
    }

    private func headline(_ snapshot: OverviewSnapshot) -> some View {
        VStack(alignment: .leading, spacing: .lg) {
            HStack(alignment: .firstTextBaseline, spacing: .xl) {
                setFigure(
                    snapshot.documentsPerMinute.formatted(
                        .number.precision(.fractionLength(0))),
                    "documents per minute")
                setFigure(snapshot.indexedDocuments.formatted(), "searchable documents")
            }

            HStack(spacing: .xl) {
                measure("staged", snapshot.stagedDocuments)
                measure("pending embedding", snapshot.pendingEmbedding)
                measure("uptime", duration(snapshot.uptimeSeconds))
                measure("version", snapshot.serviceVersion)
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

    private func runPressure(_ runs: OverviewRunCounts) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Run pressure", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            HStack(spacing: .xl) {
                measure("active", runs.active)
                measure("queued", runs.queued)
                measure("blocked", runs.blocked)
                measure("recent failures", runs.failed)
            }
        }
    }

    private func sourceTable(_ sources: [OverviewSourceStatus]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Sources", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)

            HStack {
                tableHeader("Source", width: 180)
                tableHeader("Pipeline", width: 180)
                tableHeader("Staged", width: 90, alignment: .trailing)
                tableHeader("Embedding", width: 90, alignment: .trailing)
                tableHeader("Searchable", width: 100, alignment: .trailing)
                tableHeader("State", width: 110)
                Spacer()
            }

            ForEach(sources) { row in
                Hairline()
                HStack {
                    Text(row.source.displayTitle)
                        .frame(width: 180, alignment: .leading)
                    Text(
                        "\(row.source.pipeline.pipeline) @ \(row.source.pipeline.version)"
                    )
                    .frame(width: 180, alignment: .leading)
                    Text(row.source.status.counts.staged.formatted())
                        .frame(width: 90, alignment: .trailing)
                    Text(row.source.status.counts.pendingEmbedding.formatted())
                        .frame(width: 90, alignment: .trailing)
                    Text(row.source.status.counts.searchable.formatted())
                        .frame(width: 100, alignment: .trailing)
                    StatusBadge(
                        row.source.status.activity.overviewStatus,
                        word: row.source.status.activity.rawValue)
                        .frame(width: 110, alignment: .leading)
                    Spacer()
                }
                .windexStyle(Typography.data)
                .frame(minHeight: 28)
            }
        }
    }

    private func serviceTable(_ services: [OverviewServiceStatus]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Services", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            if services.isEmpty {
                Text("No service health projection is available.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }
            ForEach(services) { service in
                HStack {
                    Text(service.name)
                        .windexStyle(Typography.label)
                        .frame(width: 180, alignment: .leading)
                    StatusBadge(
                        service.available ? .healthy : .fault,
                        word: service.available ? "available" : "unavailable")
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
                HStack(alignment: .firstTextBaseline, spacing: .sm) {
                    Text(event.timestamp, format: .dateTime.hour().minute().second())
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                        .frame(width: 80, alignment: .leading)
                    StatusBadge(.fault, word: event.level.rawValue)
                    Text(event.message)
                        .windexStyle(Typography.body)
                }
            }
        }
    }

    private var waitingState: some View {
        VStack(alignment: .leading, spacing: .md) {
            StyledText("Awaiting the all-up projection", Typography.setLG)
            Text(
                "The canonical Overview snapshot will combine throughput, Run pressure, Source progress, corpus stages, recent failures, and service availability. Known data will remain visible while it refreshes."
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

    private func duration(_ seconds: Int) -> String {
        Duration.seconds(seconds).formatted(
            .units(allowed: [.days, .hours, .minutes], width: .abbreviated))
    }
}

private extension SourceActivityState {
    var overviewStatus: Status {
        switch self {
        case .idle:
            .healthy
        case .queued, .running:
            .running
        case .blocked, .paused:
            .attention
        case .failed, .archived:
            .fault
        }
    }
}
