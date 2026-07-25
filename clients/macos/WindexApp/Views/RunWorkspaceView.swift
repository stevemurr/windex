import SwiftUI
import WindexKit
import WindexUI

struct RunsView: View {
    @Environment(BackendSession.self) private var session
    @State private var selectedRunID: Int?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        HSplitView {
            catalogue
                .frame(minWidth: 260, idealWidth: 320, maxWidth: 380)
            detail
                .frame(minWidth: 560)
        }
        .background(theme.palette.ink)
        .onChange(of: session.runs.runs) { _, runs in
            if selectedRunID == nil {
                selectedRunID = runs.first?.id
            } else if !runs.contains(where: { $0.id == selectedRunID }) {
                selectedRunID = runs.first?.id
            }
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack {
                StyledText("Runs", Typography.masthead)
                Spacer()
                liveStatus
            }
            .padding(.md)
            Hairline()

            if session.runs.runs.isEmpty {
                VStack(alignment: .leading, spacing: .sm) {
                    Text("No Runs yet.")
                        .windexStyle(Typography.label)
                    Text(
                        "Run a Source or a published generic Pipeline to see its frozen revision, progress, outputs, and Events here."
                    )
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
                    Spacer()
                }
                .padding(.lg)
            } else {
                List(session.runs.runs, selection: $selectedRunID) { run in
                    CanonicalRunRow(run: run)
                        .tag(run.id)
                }
                .listStyle(.sidebar)
                .scrollContentBackground(.hidden)
            }
        }
        .background(theme.palette.plate)
    }

    @ViewBuilder
    private var liveStatus: some View {
        switch session.events.connection {
        case .live:
            StatusBadge(.running, word: "live")
        case .degraded:
            StatusBadge(.attention, word: "reconciling")
        case .idle, .awaitingContract, .connecting:
            StatusBadge(.attention, word: "awaiting stream")
        }
    }

    @ViewBuilder
    private var detail: some View {
        if let selectedRunID,
           let run = session.runs.runs.first(where: { $0.id == selectedRunID }) {
            CanonicalRunDetail(run: run)
        } else {
            Text("Choose a Run.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }
}

private struct CanonicalRunRow: View {
    let run: SourceRunSummary
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            HStack {
                Text(run.sourceName ?? "Generic Pipeline Run")
                    .windexStyle(Typography.label)
                Spacer()
                StatusBadge(run.state.badgeStatus, word: run.state.rawValue)
            }
            Text(
                "\(run.pipeline.pipeline) @ \(run.pipeline.version) · run \(run.id) · \(run.flow)"
            )
            .windexStyle(Typography.dataSM)
            .foregroundStyle(theme.palette.graphite)
        }
        .padding(.vertical, .xxs)
        .accessibilityElement(children: .combine)
    }
}

private struct CanonicalRunDetail: View {
    let run: SourceRunSummary
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                HStack(alignment: .firstTextBaseline) {
                    VStack(alignment: .leading, spacing: .xs) {
                        StyledText("Run \(run.id)", Typography.setLG)
                        Text(run.sourceName ?? "generic Pipeline Run")
                            .windexStyle(Typography.data)
                            .foregroundStyle(theme.palette.graphite)
                    }
                    Spacer()
                    StatusBadge(run.state.badgeStatus, word: run.state.rawValue)
                }

                Hairline()
                dataRow("Pipeline", "\(run.pipeline.pipeline) @ \(run.pipeline.version)")
                dataRow("Revision hash", run.pipeline.specHash)
                dataRow("Flow", run.flow)
                if let progress = run.progress {
                    VStack(alignment: .leading, spacing: .xs) {
                        Text("Progress")
                            .windexStyle(Typography.label)
                            .foregroundStyle(theme.palette.graphite)
                        ProgressView(value: progress)
                        Text(progress.formatted(.percent.precision(.fractionLength(0))))
                            .windexStyle(Typography.dataSM)
                    }
                }

                HStack(spacing: .sm) {
                    Button("Re-run") {}
                        .disabled(true)
                        .help("Executes this historic frozen revision and configuration.")
                    if run.sourceName != nil {
                        Button("Run latest") {}
                            .disabled(true)
                            .help("Executes the Source’s current revision and configuration.")
                    }
                }

                if let error = run.error {
                    Hairline()
                    StyledText("Failure", Typography.eyebrow)
                        .foregroundStyle(theme.palette.rust)
                    Text(error)
                        .windexStyle(Typography.data)
                        .textSelection(.enabled)
                }

                Hairline()
                Text(
                    "Task progress, declared inputs and outputs, artifacts, and Events will populate from the canonical Run projection."
                )
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
            }
            .padding(.xl)
        }
    }

    private func dataRow(_ label: String, _ value: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: .md) {
            Text(label)
                .windexStyle(Typography.label)
                .foregroundStyle(theme.palette.graphite)
                .frame(width: 120, alignment: .leading)
            Text(value)
                .windexStyle(Typography.data)
                .textSelection(.enabled)
            Spacer()
        }
    }
}

private extension SourceActivityState {
    var badgeStatus: Status {
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
