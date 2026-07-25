import Foundation
import Observation
import SwiftUI
import WindexKit
import WindexUI

struct ColophonSnapshot: Equatable, Sendable {
    let embedsPerMinute: Double
    let documentCount: Int
    let uptimeSeconds: Int
    let sources: [SourceRow]

    init(
        embedsPerMinute: Double,
        documentCount: Int,
        uptimeSeconds: Int,
        sources: [SourceRow]
    ) {
        self.embedsPerMinute = embedsPerMinute
        self.documentCount = documentCount
        self.uptimeSeconds = uptimeSeconds
        self.sources = sources.sorted {
            $0.name.localizedStandardCompare($1.name) == .orderedAscending
        }
    }

    init(stats: JSONValue, freshness: [SourceFreshness]) {
        let root = stats.objectValue ?? [:]
        let activity = root["activity"]?.objectValue ?? [:]
        let totals = root["totals"]?.objectValue ?? [:]

        embedsPerMinute = activity["docs_per_min"]?.doubleValue ?? 0
        documentCount = totals["indexed_pages"]?.intValue
            ?? freshness.reduce(0) { $0 + ($1.indexed ?? 0) }
        uptimeSeconds = activity["uptime_s"]?.intValue ?? 0
        sources = freshness
            .map { SourceRow($0) }
            .sorted { $0.name.localizedStandardCompare($1.name) == .orderedAscending }
    }
}

struct SourceRow: Equatable, Identifiable, Sendable {
    enum Condition: Equatable, Sendable {
        case healthy
        case running
        case attention(String)
    }

    let name: String
    let indexed: Int
    let pending: Int
    let lastActivity: Date?
    let condition: Condition

    var id: String { name }

    init(_ freshness: SourceFreshness, now: Date = Date()) {
        let latest = max(freshness.lastUpdateTs ?? 0, freshness.lastEmbedTs ?? 0)
        self.init(
            name: freshness.source,
            indexed: freshness.indexed ?? 0,
            pending: freshness.pending ?? 0,
            lastActivity: latest > 0 ? Date(timeIntervalSince1970: latest) : nil,
            now: now)
    }

    init(
        name: String,
        indexed: Int,
        pending: Int,
        lastActivity: Date?,
        now: Date = Date()
    ) {
        self.name = name
        self.indexed = indexed
        self.pending = pending
        self.lastActivity = lastActivity
        let age = lastActivity.map { now.timeIntervalSince($0) }

        if pending > 0, let age, age < 5 * 60 {
            condition = .running
        } else if pending > 0 {
            condition = .attention("queued")
        } else if let age, age > 24 * 60 * 60 {
            condition = .attention("stale")
        } else {
            condition = .healthy
        }
    }
}

@MainActor
@Observable
final class OverviewModel {
    private(set) var snapshot: ColophonSnapshot?
    private(set) var isLoading = false
    private(set) var liveUpdatesAvailable = true
    private(set) var errorMessage: String?

    func run(client: WindexClient, appModel: AppModel) async {
        isLoading = snapshot == nil
        guard await refresh(client: client, appModel: appModel) else { return }

        do {
            let stream = try await client.dashboardEvents()
            for try await event in stream {
                try Task.checkCancellation()
                if case .stats(let stats) = event {
                    let freshness = try await client.freshness()
                    snapshot = ColophonSnapshot(stats: stats, freshness: freshness)
                    errorMessage = nil
                }
            }
            try Task.checkCancellation()
            liveUpdatesAvailable = false
        } catch is CancellationError {
            return
        } catch {
            appModel.handleClientError(error)
            guard appModel.connectedBackend != nil else { return }
            liveUpdatesAvailable = false
        }

        while !Task.isCancelled {
            do {
                try await Task.sleep(for: .seconds(5))
            } catch {
                return
            }
            _ = await refresh(client: client, appModel: appModel)
        }
    }

    @discardableResult
    func refresh(client: WindexClient, appModel: AppModel) async -> Bool {
        do {
            async let stats = client.stats()
            async let freshness = client.freshness()
            snapshot = try await ColophonSnapshot(
                stats: stats,
                freshness: freshness)
            errorMessage = nil
            isLoading = false
            return true
        } catch {
            isLoading = false
            appModel.handleClientError(error)
            guard appModel.connectedBackend != nil else { return false }
            errorMessage = Self.presentation(for: error)
            return false
        }
    }

    private static func presentation(for error: any Error) -> String {
        guard let error = error as? WindexError else {
            return "The overview could not be refreshed."
        }
        switch error {
        case .transport:
            return "The backend stopped answering. Check the network, then retry."
        case .adminDisabled(let message):
            return message
        default:
            return error.localizedDescription
        }
    }
}

struct OverviewView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend

    @State private var model = OverviewModel()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                runningHead
                Hairline()

                if let snapshot = model.snapshot {
                    metrics(snapshot)
                    Hairline()
                    sourceTable(snapshot.sources)
                    Hairline()
                    footer
                } else if model.isLoading {
                    ProgressView()
                        .controlSize(.small)
                        .frame(maxWidth: .infinity, minHeight: 280)
                        .accessibilityLabel("Loading overview")
                } else {
                    failure
                }
            }
            .padding(.xl)
            .frame(maxWidth: 1060, alignment: .leading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(theme.palette.ink)
        .task(id: backend.profile) {
            await model.run(client: client, appModel: appModel)
        }
    }

    private var runningHead: some View {
        HStack(alignment: .firstTextBaseline) {
            StyledText("Windex", Typography.masthead)
            Spacer()
            if !model.liveUpdatesAvailable {
                Text("live updates unavailable — refreshing every 5s")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            } else {
                Text("running · \(uptime(backend.evidence.uptimeSeconds))")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
        }
    }

    private func metrics(_ snapshot: ColophonSnapshot) -> some View {
        HStack(alignment: .top, spacing: .xxl) {
            VStack(alignment: .leading, spacing: .xxs) {
                Text(snapshot.embedsPerMinute.formatted(.number.precision(.fractionLength(0...1))))
                    .windexStyle(Typography.setXL)
                    .contentTransition(.numericText())
                    .windexAnimation(Motion.counter, value: snapshot.embedsPerMinute)
                Text("embeds per minute")
                    .windexStyle(Typography.label)
                    .foregroundStyle(theme.palette.graphite)
            }
            .accessibilityElement(children: .combine)

            VStack(alignment: .leading, spacing: .xxs) {
                Text(snapshot.documentCount.formatted())
                    .windexStyle(Typography.setLG)
                    .contentTransition(.numericText())
                    .windexAnimation(Motion.counter, value: snapshot.documentCount)
                Text("documents across \(snapshot.sources.count) sources")
                    .windexStyle(Typography.label)
                    .foregroundStyle(theme.palette.graphite)
            }
            .padding(.top, .sm)
            .accessibilityElement(children: .combine)

            Spacer()
        }
    }

    private func sourceTable(_ rows: [SourceRow]) -> some View {
        ScrollView(.horizontal) {
            Grid(alignment: .leading, horizontalSpacing: 24, verticalSpacing: 0) {
                GridRow {
                    header("Source", width: 140, alignment: .leading)
                    header("Indexed", width: 112, alignment: .trailing)
                    header("Pending", width: 96, alignment: .trailing)
                    header("Last activity", width: 120, alignment: .leading)
                    header("State", width: 104, alignment: .leading)
                }
                .padding(.bottom, .xs)

                ForEach(rows) { row in
                    Hairline()
                        .gridCellColumns(5)
                    GridRow {
                        Text(row.name)
                            .frame(width: 140, alignment: .leading)
                        Text(row.indexed.formatted())
                            .frame(width: 112, alignment: .trailing)
                        Text(row.pending.formatted())
                            .frame(width: 96, alignment: .trailing)
                        Text(relative(row.lastActivity))
                            .frame(width: 120, alignment: .leading)
                        condition(row.condition)
                            .frame(width: 104, alignment: .leading)
                    }
                    .windexStyle(Typography.data)
                    .padding(.vertical, .xs)
                    .accessibilityElement(children: .combine)
                }
            }
        }
    }

    private func header(
        _ value: String,
        width: CGFloat,
        alignment: Alignment
    ) -> some View {
        StyledText(value, Typography.eyebrow)
            .foregroundStyle(theme.palette.graphite)
            .frame(width: width, alignment: alignment)
    }

    @ViewBuilder
    private func condition(_ condition: SourceRow.Condition) -> some View {
        switch condition {
        case .healthy:
            StatusBadge(.healthy)
        case .running:
            StatusBadge(.running)
        case .attention(let word):
            StatusBadge(.attention, word: word)
        }
    }

    private var footer: some View {
        Text(
            "gateway ok · admin scope confirmed · windex \(backend.evidence.version ?? "unknown")")
            .windexStyle(Typography.dataSM)
            .foregroundStyle(theme.palette.graphite)
    }

    private var failure: some View {
        VStack(alignment: .leading, spacing: .sm) {
            Text(model.errorMessage ?? "The overview could not be loaded.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.rust)
                .textSelection(.enabled)
            HStack(spacing: .sm) {
                Button("Retry") {
                    Task {
                        await model.refresh(client: client, appModel: appModel)
                    }
                }
                Button("Change backend") {
                    appModel.changeBackend()
                }
            }
            .buttonStyle(.bordered)
        }
        .frame(maxWidth: Layout.proseMeasure, minHeight: 280, alignment: .center)
    }

    private func relative(_ date: Date?) -> String {
        guard let date else { return "never" }
        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .abbreviated
        return formatter.localizedString(for: date, relativeTo: Date())
    }

    private func uptime(_ seconds: Int) -> String {
        let days = seconds / 86_400
        let hours = (seconds % 86_400) / 3_600
        let minutes = (seconds % 3_600) / 60
        if days > 0 { return "\(days)d \(hours)h" }
        if hours > 0 { return "\(hours)h \(minutes)m" }
        return "\(minutes)m"
    }
}
