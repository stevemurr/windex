import Observation
import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class RunsModel {
    private(set) var runs: [RecipeRun] = []
    private(set) var detail: RecipeRun?
    private(set) var events: [RecipeRunEvent] = []
    private(set) var isLoading = false
    private(set) var isActing = false
    private(set) var liveUnavailable = false
    private(set) var errorMessage: String?
    var selectedRunID: Int?

    func load(client: WindexClient, appModel: AppModel) async {
        isLoading = runs.isEmpty
        do {
            runs = try await client.runs(limit: 100)
            isLoading = false
            errorMessage = nil
            if selectedRunID == nil {
                selectedRunID = runs.first?.id
            }
        } catch {
            isLoading = false
            present(error, appModel: appModel)
        }
    }

    func monitor(client: WindexClient, appModel: AppModel) async {
        guard let id = selectedRunID else {
            detail = nil
            events = []
            return
        }
        await refresh(id: id, client: client, appModel: appModel)
        while !Task.isCancelled, selectedRunID == id, detail?.isTerminal != true {
            do {
                let cursor = events.last?.seq ?? 0
                let updates = try await client.runUpdates(id: id, after: cursor)
                liveUnavailable = false
                for try await update in updates {
                    guard selectedRunID == id, !Task.isCancelled else { return }
                    switch update {
                    case .run(let run):
                        adopt(run)
                    case .events(let rows):
                        append(rows)
                    case .end:
                        return
                    case .serverError(let message):
                        errorMessage = message
                        return
                    case .unknown:
                        break
                    }
                }
            } catch {
                guard selectedRunID == id, !Task.isCancelled else { return }
                appModel.handleClientError(error)
                guard appModel.connectedBackend != nil else { return }
                liveUnavailable = true
            }
            do {
                try await Task.sleep(for: .seconds(5))
            } catch {
                return
            }
            await refresh(id: id, client: client, appModel: appModel, quietly: true)
        }
    }

    func cancel(client: WindexClient, appModel: AppModel) async {
        guard let id = selectedRunID else { return }
        isActing = true
        defer { isActing = false }
        do {
            _ = try await client.cancelRun(id: id)
            await refresh(id: id, client: client, appModel: appModel)
            await load(client: client, appModel: appModel)
        } catch {
            present(error, appModel: appModel)
        }
    }

    func rerun(client: WindexClient, appModel: AppModel) async {
        guard let detail else { return }
        isActing = true
        defer { isActing = false }
        do {
            var params = try detail.parameters()
            let flow = params.removeValue(forKey: "flow")?.stringValue
            let queued = try await client.createRun(
                recipe: detail.recipe, flow: flow, params: params)
            if let runID = queued.runId {
                await load(client: client, appModel: appModel)
                selectedRunID = runID
            } else {
                errorMessage = "A live run already holds this recipe’s queue key."
            }
        } catch {
            present(error, appModel: appModel)
        }
    }

    private func refresh(
        id: Int,
        client: WindexClient,
        appModel: AppModel,
        quietly: Bool = false
    ) async {
        if !quietly { isLoading = detail == nil }
        do {
            async let run = client.run(id: id)
            async let rows = client.runEvents(id: id, limit: 500)
            let (loaded, loadedEvents) = try await (run, rows)
            guard selectedRunID == id else { return }
            adopt(loaded)
            events = loadedEvents
            isLoading = false
            errorMessage = nil
        } catch {
            guard selectedRunID == id else { return }
            isLoading = false
            present(error, appModel: appModel)
        }
    }

    private func adopt(_ run: RecipeRun) {
        detail = run
        if let index = runs.firstIndex(where: { $0.id == run.id }) {
            runs[index] = run
        }
    }

    private func append(_ rows: [RecipeRunEvent]) {
        let cursor = events.last?.seq ?? 0
        events.append(contentsOf: rows.filter { $0.seq > cursor })
        if events.count > 500 {
            events.removeFirst(events.count - 500)
        }
    }

    private func present(_ error: any Error, appModel: AppModel) {
        appModel.handleClientError(error)
        guard appModel.connectedBackend != nil else { return }
        errorMessage = (error as? WindexError)?.localizedDescription
            ?? "Run history could not be loaded."
    }
}

struct RunsView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend

    @State private var model = RunsModel()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        GeometryReader { geometry in
            if geometry.size.width >= 850 {
                HSplitView {
                    catalogue
                        .frame(minWidth: 260, idealWidth: 320, maxWidth: 380)
                    galley
                        .frame(minWidth: 560)
                }
            } else if model.selectedRunID == nil {
                catalogue
            } else {
                galley
            }
        }
        .background(theme.palette.ink)
        .task(id: backend.profile) {
            await model.load(client: client, appModel: appModel)
        }
        .task(id: model.selectedRunID) {
            await model.monitor(client: client, appModel: appModel)
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack {
                StyledText("Runs", Typography.masthead)
                Spacer()
                Button {
                    Task { await model.load(client: client, appModel: appModel) }
                } label: {
                    Image(systemName: "arrow.clockwise")
                        .accessibilityLabel("Refresh runs")
                }
                .buttonStyle(.plain)
                .disabled(model.isLoading)
            }
            .padding(.md)
            Hairline()

            if model.isLoading, model.runs.isEmpty {
                ProgressView()
                    .controlSize(.small)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if model.runs.isEmpty {
                VStack(alignment: .leading, spacing: .sm) {
                    Text("No runs yet.")
                        .windexStyle(Typography.label)
                    Text("Run a source recipe to see its tasks, progress, and events here.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                    Spacer()
                }
                .padding(.lg)
                .frame(maxWidth: .infinity, alignment: .leading)
            } else {
                List(model.runs, id: \.id, selection: selection) { run in
                    RunCatalogueRow(run: run)
                        .tag(run.id)
                }
                .listStyle(.sidebar)
                .scrollContentBackground(.hidden)
            }
        }
        .background(theme.palette.plate)
    }

    private var selection: Binding<Int?> {
        Binding(get: { model.selectedRunID },
                set: { model.selectedRunID = $0 })
    }

    @ViewBuilder
    private var galley: some View {
        if model.isLoading, model.detail == nil {
            ProgressView()
                .controlSize(.small)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else if let run = model.detail {
            RunGalleyView(
                run: run,
                events: model.events,
                liveUnavailable: model.liveUnavailable,
                isActing: model.isActing,
                cancel: {
                    Task { await model.cancel(client: client, appModel: appModel) }
                },
                rerun: {
                    Task { await model.rerun(client: client, appModel: appModel) }
                })
        } else if let error = model.errorMessage {
            SourceFailureView(message: error) {
                Task { await model.load(client: client, appModel: appModel) }
            }
        } else {
            Text("Choose a run.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }
}

private struct RunCatalogueRow: View {
    let run: RecipeRun
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            HStack {
                Text(run.recipe)
                    .windexStyle(Typography.label)
                Spacer()
                RunStatusBadge(state: run.state)
            }
            HStack(spacing: .xs) {
                Text("#\(run.id)")
                Text("·")
                Text(run.trigger ?? "manual")
                if let queued = run.queuedAt {
                    Text("· \(queued.prefix(19))")
                }
            }
            .windexStyle(Typography.dataSM)
            .foregroundStyle(theme.palette.graphite)
        }
        .padding(.vertical, .xxs)
        .accessibilityElement(children: .combine)
    }
}

private struct RunGalleyView: View {
    let run: RecipeRun
    let events: [RecipeRunEvent]
    let liveUnavailable: Bool
    let isActing: Bool
    let cancel: () -> Void
    let rerun: () -> Void
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                header
                RunProgressBar(tasks: run.tasks ?? [])
                Hairline()
                taskGalley
                Hairline()
                eventLog
            }
            .padding(.xl)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .background(theme.palette.ink)
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: .sm) {
            HStack(alignment: .firstTextBaseline) {
                StyledText("\(run.recipe) · run \(run.id)", Typography.setLG)
                Spacer()
                RunStatusBadge(state: run.state)
            }
            HStack(spacing: .sm) {
                if run.isTerminal {
                    Button("Re-run", action: rerun)
                        .buttonStyle(.bordered)
                        .disabled(isActing)
                } else {
                    Button("Stop", role: .destructive, action: cancel)
                        .buttonStyle(.bordered)
                        .disabled(isActing || run.cancelRequested == true)
                }
                if liveUnavailable {
                    StatusBadge(.attention,
                                word: "live updates unavailable — refreshing every 5s")
                }
            }
            if let error = run.error, !error.isEmpty {
                Text(error)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.rust)
                    .textSelection(.enabled)
            }
        }
    }

    private var taskGalley: some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Tasks", Typography.eyebrow)
            ForEach(Array((run.tasks ?? []).enumerated()), id: \.element.id) {
                index, task in
                RunTaskRow(number: index + 1, task: task)
            }
        }
    }

    private var eventLog: some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Events", Typography.eyebrow)
            if events.isEmpty {
                Text("No events recorded.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            } else {
                ForEach(events.suffix(100), id: \.seq) { event in
                    HStack(alignment: .firstTextBaseline, spacing: .sm) {
                        Text(String(event.timestamp.prefix(19)))
                            .frame(width: 148, alignment: .leading)
                        Text(event.event)
                            .frame(width: 150, alignment: .leading)
                        Text(event.message)
                            .foregroundStyle(
                                event.level == "error"
                                    ? theme.palette.rust : theme.palette.graphite)
                    }
                    .windexStyle(Typography.dataSM)
                    .textSelection(.enabled)
                }
            }
        }
    }
}

private struct RunTaskRow: View {
    let number: Int
    let task: RecipeRunTask
    @Environment(\.windexTheme) private var theme

    var body: some View {
        HStack(alignment: .top, spacing: .md) {
            Text(String(format: "%02d", number))
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
                .frame(width: 28, alignment: .trailing)
            VStack(alignment: .leading, spacing: .xxs) {
                HStack {
                    Text(task.node)
                        .windexStyle(Typography.label)
                    Spacer()
                    RunStatusBadge(state: task.state)
                }
                HStack(spacing: .xs) {
                    Text(task.module)
                    if let lane = task.lane { Text("· \(lane)") }
                    if let total = task.unitsTotal, total >= 0 {
                        Text("· \(task.unitsDone ?? 0) of \(total)")
                    } else if let done = task.unitsDone, done > 0 {
                        Text("· \(done) units")
                    }
                    if let failed = task.unitsFailed, failed > 0 {
                        Text("· \(failed) failed")
                            .foregroundStyle(theme.palette.rust)
                    }
                }
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
                if let error = task.error, !error.isEmpty {
                    Text(error)
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.rust)
                }
            }
            .padding(.sm)
            .background(theme.palette.plate)
            .overlay(Rectangle().stroke(theme.palette.rule, lineWidth: 1))
        }
    }
}

private struct RunProgressBar: View {
    let tasks: [RecipeRunTask]
    @Environment(\.windexTheme) private var theme

    private var fraction: Double {
        let counted = tasks.filter { ($0.unitsTotal ?? -1) > 0 }
        guard !counted.isEmpty else {
            let complete = tasks.filter {
                ["succeeded", "skipped"].contains($0.state)
            }.count
            return tasks.isEmpty ? 0 : Double(complete) / Double(tasks.count)
        }
        let done = counted.reduce(0) { $0 + min($1.unitsDone ?? 0, $1.unitsTotal ?? 0) }
        let total = counted.reduce(0) { $0 + ($1.unitsTotal ?? 0) }
        return total > 0 ? Double(done) / Double(total) : 0
    }

    var body: some View {
        VStack(alignment: .leading, spacing: .xs) {
            GeometryReader { geometry in
                ZStack(alignment: .leading) {
                    Rectangle().fill(theme.palette.rule)
                    Rectangle()
                        .fill(theme.palette.cyan)
                        .frame(width: geometry.size.width * max(0, min(1, fraction)))
                }
            }
            .frame(height: 3)
            Text("\(Int(fraction * 100))% · \(tasks.count) tasks")
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
        }
    }
}

private struct RunStatusBadge: View {
    let state: String

    var body: some View {
        switch state {
        case "running":
            StatusBadge(.running, word: "running")
        case "failed":
            StatusBadge(.fault, word: "failed")
        case "cancelled":
            StatusBadge(.attention, word: "cancelled")
        case "blocked":
            StatusBadge(.attention, word: "blocked")
        case "queued", "pending", "ready":
            StatusBadge(.attention, word: state)
        case "succeeded", "skipped":
            StatusBadge(.healthy, word: state == "skipped" ? "skipped" : nil)
        default:
            StatusBadge(.healthy, word: state)
        }
    }
}
