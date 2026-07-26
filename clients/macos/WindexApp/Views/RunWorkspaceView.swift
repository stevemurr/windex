import AppKit
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
        case .idle, .connecting:
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
    @Environment(BackendSession.self) private var session
    @State private var isMutating = false
    @State private var actionError: String?
    @State private var artifactMessage: String?
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
                    Button("Re-run") {
                        Task { await perform { try await session.rerunFrozen(runID: run.id) } }
                    }
                    .disabled(isMutating)
                        .help("Executes this historic frozen revision and configuration.")
                    if run.sourceName != nil {
                        Button("Run latest") {
                            Task {
                                await perform {
                                    try await session.runLatest(source: run.sourceName!)
                                }
                            }
                        }
                        .disabled(isMutating)
                            .help("Executes the Source’s current revision and configuration.")
                    }
                    if run.state == .queued || run.state == .running {
                        Button("Cancel", role: .destructive) {
                            Task { await perform { try await session.cancel(runID: run.id) } }
                        }
                        .disabled(isMutating)
                    }
                }
                if let actionError {
                    Text(actionError)
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.rust)
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
                runProjection
            }
            .padding(.xl)
        }
        .task(id: "\(run.id):\(run.state.rawValue)") {
            await session.loadRunDetail(run.id)
        }
    }

    @ViewBuilder
    private var runProjection: some View {
        if let detail = session.runs.details[run.id] {
            taskProgress(detail.tasks ?? [])
            Hairline()
            outputs(session.runs.outputs[run.id] ?? [])
            Hairline()
            runEvents(session.runs.events[run.id] ?? [])
        } else if let error = session.runs.detailErrors[run.id] {
            VStack(alignment: .leading, spacing: .sm) {
                Text(error)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.rust)
                Button("Retry") {
                    Task { await session.loadRunDetail(run.id) }
                }
            }
        } else {
            HStack(spacing: .sm) {
                ProgressView().controlSize(.small)
                Text("Loading tasks, Events, outputs, and artifacts…")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }
        }
    }

    private func taskProgress(_ tasks: [RunTaskWire]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Tasks", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            if tasks.isEmpty {
                Text("This Run has no executable tasks.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }
            ForEach(tasks, id: \.id) { task in
                VStack(alignment: .leading, spacing: .xs) {
                    HStack {
                        Text(task.node)
                            .windexStyle(Typography.label)
                        Text("\(task.module) · \(task.lane)")
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.graphite)
                        Spacer()
                        StatusBadge(
                            status(for: task.state),
                            word: task.state
                        )
                    }
                    if task.unitsTotal > 0 {
                        ProgressView(
                            value: Double(task.unitsDone + task.unitsFailed),
                            total: Double(task.unitsTotal)
                        )
                        Text(
                            "\(task.unitsDone) done · \(task.unitsFailed) failed · "
                                + "\(task.unitsTotal) total"
                        )
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                    } else if task.state == "running" {
                        ProgressView()
                            .controlSize(.small)
                    }
                    if let error = task.error {
                        Text(error)
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.rust)
                            .textSelection(.enabled)
                    }
                }
                .padding(.sm)
                .background(theme.palette.plate)
            }
        }
    }

    private func outputs(_ values: [RunOutputWire]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Outputs and artifacts", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            if values.isEmpty {
                Text("No declared boundary outputs have been captured.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }
            ForEach(values, id: \.boundary) { output in
                VStack(alignment: .leading, spacing: .xs) {
                    HStack {
                        Text(output.boundary)
                            .windexStyle(Typography.label)
                        Text(output._type)
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.graphite)
                        Spacer()
                        Text(ByteCountFormatter.string(
                            fromByteCount: Int64(output.sizeBytes),
                            countStyle: .file
                        ))
                        .windexStyle(Typography.dataSM)
                        if let artifactID = output.artifactID {
                            Button("Save artifact…") {
                                Task {
                                    await saveArtifact(
                                        artifactID,
                                        boundary: output.boundary
                                    )
                                }
                            }
                            .buttonStyle(.borderless)
                        }
                    }
                    Text(outputValue(output))
                        .font(.system(.caption, design: .monospaced))
                        .lineLimit(8)
                        .textSelection(.enabled)
                    Text("sha256 \(output.checksum)")
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                }
                .padding(.sm)
                .background(theme.palette.plate)
            }
            if let artifactMessage {
                Text(artifactMessage)
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
        }
    }

    private func runEvents(_ events: [OperationalEvent]) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Events", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            if events.isEmpty {
                Text("No Events recorded for this Run.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }
            ForEach(events) { event in
                HStack(alignment: .firstTextBaseline, spacing: .sm) {
                    Text(event.timestamp, format: .dateTime.hour().minute().second())
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                        .frame(width: 80, alignment: .leading)
                    Text(event.event)
                        .windexStyle(Typography.label)
                        .frame(width: 150, alignment: .leading)
                    Text(event.message)
                        .windexStyle(Typography.body)
                    Spacer()
                }
            }
        }
    }

    private func outputValue(_ output: RunOutputWire) -> String {
        guard let value = try? output.decodedValue(),
              let data = try? JSONEncoder.pretty.encode(value),
              let result = String(data: data, encoding: .utf8) else {
            return "Value unavailable"
        }
        return result
    }

    private func saveArtifact(_ artifactID: String, boundary: String) async {
        do {
            let data = try await session.artifact(
                runID: run.id,
                artifactID: artifactID
            )
            let panel = NSSavePanel()
            panel.nameFieldStringValue = "\(boundary)-\(artifactID)"
            guard panel.runModal() == .OK, let url = panel.url else { return }
            try data.write(to: url, options: .atomic)
            artifactMessage = "Saved \(url.lastPathComponent)."
        } catch {
            artifactMessage = error.localizedDescription
        }
    }

    private func status(for state: String) -> Status {
        switch state {
        case "succeeded", "skipped":
            .healthy
        case "ready", "running":
            .running
        case "blocked", "queued":
            .attention
        default:
            .fault
        }
    }

    private func perform(_ action: () async throws -> Void) async {
        isMutating = true
        defer { isMutating = false }
        do {
            try await action()
            actionError = nil
        } catch {
            actionError = error.localizedDescription
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

private extension JSONEncoder {
    static var pretty: JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return encoder
    }
}

private extension SourceActivityState {
    var badgeStatus: Status {
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
