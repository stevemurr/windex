import Observation
import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class SourcesModel {
    private(set) var recipes: [Recipe] = []
    private(set) var detail: Recipe?
    private(set) var flows: [String: RecipeFlowSummary] = [:]
    private(set) var tasks: [RecipeTask] = []
    private(set) var executable = false
    private(set) var unavailableModules: [String] = []
    private(set) var isLoading = false
    private(set) var isLoadingDetail = false
    private(set) var isActing = false
    private(set) var errorMessage: String?
    var selectedName: String?
    var selectedFlow: String?

    func load(client: WindexClient, appModel: AppModel) async {
        isLoading = recipes.isEmpty
        do {
            let loaded = try await client.recipes()
            recipes = loaded.sorted {
                if ($0.builtin ?? false) != ($1.builtin ?? false) {
                    return ($0.builtin ?? false) && !($1.builtin ?? false)
                }
                return $0.displayTitle.localizedStandardCompare($1.displayTitle)
                    == .orderedAscending
            }
            isLoading = false
            errorMessage = nil

            if let selectedName,
               recipes.contains(where: { $0.name == selectedName }) {
                await select(selectedName, client: client, appModel: appModel)
            } else if let first = recipes.first {
                await select(first.name, client: client, appModel: appModel)
            }
        } catch {
            isLoading = false
            present(error, appModel: appModel)
        }
    }

    func select(
        _ name: String,
        client: WindexClient,
        appModel: AppModel
    ) async {
        selectedName = name
        detail = nil
        flows = [:]
        tasks = []
        executable = false
        unavailableModules = []
        selectedFlow = nil
        isLoadingDetail = true

        do {
            let recipe = try await client.recipe(named: name)
            let decodedFlows = try recipe.flowSummaries()
            guard selectedName == name else { return }

            detail = recipe
            flows = decodedFlows
            let flow = preferredFlow(in: decodedFlows)
            selectedFlow = flow
            if let flow {
                let response = try await client.recipeTasks(named: name, flow: flow)
                guard selectedName == name, selectedFlow == flow else { return }
                tasks = try response.placements()
                executable = response.executable ?? false
                unavailableModules = response.unavailableModules ?? []
            }
            isLoadingDetail = false
            errorMessage = nil
        } catch {
            guard selectedName == name else { return }
            isLoadingDetail = false
            present(error, appModel: appModel)
        }
    }

    func selectFlow(
        _ flow: String,
        client: WindexClient,
        appModel: AppModel
    ) async {
        guard let name = selectedName else { return }
        selectedFlow = flow
        tasks = []
        isLoadingDetail = true
        do {
            let response = try await client.recipeTasks(named: name, flow: flow)
            guard selectedName == name, selectedFlow == flow else { return }
            tasks = try response.placements()
            executable = response.executable ?? false
            unavailableModules = response.unavailableModules ?? []
            isLoadingDetail = false
            errorMessage = nil
        } catch {
            guard selectedName == name, selectedFlow == flow else { return }
            isLoadingDetail = false
            present(error, appModel: appModel)
        }
    }

    func run(client: WindexClient, appModel: AppModel) async {
        guard executable, let selectedName else { return }
        isActing = true
        defer { isActing = false }
        do {
            let queued = try await client.createRun(
                recipe: selectedName,
                flow: selectedFlow)
            if queued.runId != nil {
                appModel.selection = .runs
            } else {
                errorMessage = "A live run already holds this recipe’s queue key."
            }
        } catch {
            present(error, appModel: appModel)
        }
    }

    private func preferredFlow(in flows: [String: RecipeFlowSummary]) -> String? {
        if flows["discover"] != nil { return "discover" }
        if flows["refresh"] != nil { return "refresh" }
        return flows.keys.sorted().first
    }

    private func present(_ error: any Error, appModel: AppModel) {
        appModel.handleClientError(error)
        guard appModel.connectedBackend != nil else { return }
        if let error = error as? WindexError {
            errorMessage = error.localizedDescription
        } else {
            errorMessage = "The source catalogue could not be loaded."
        }
    }
}

struct SourcesView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend

    @State private var model = SourcesModel()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        GeometryReader { geometry in
            if geometry.size.width >= 800 {
                HSplitView {
                    catalogue
                        .frame(minWidth: 220, idealWidth: 260, maxWidth: 320)
                    detail
                        .frame(minWidth: 520)
                }
            } else if model.selectedName == nil {
                catalogue
            } else {
                detail
                    .toolbar {
                        ToolbarItem(placement: .navigation) {
                            Button {
                                model.selectedName = nil
                            } label: {
                                Label("All sources", systemImage: "chevron.left")
                            }
                        }
                    }
            }
        }
        .background(theme.palette.ink)
        .task(id: backend.profile) {
            await model.load(client: client, appModel: appModel)
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack(alignment: .firstTextBaseline) {
                StyledText("Sources", Typography.masthead)
                Spacer()
                Button {
                    Task {
                        await model.load(client: client, appModel: appModel)
                    }
                } label: {
                    Image(systemName: "arrow.clockwise")
                        .accessibilityLabel("Refresh sources")
                }
                .buttonStyle(.plain)
                .disabled(model.isLoading)
            }
            .padding(.md)

            Hairline()

            if model.isLoading && model.recipes.isEmpty {
                ProgressView()
                    .controlSize(.small)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .accessibilityLabel("Loading sources")
            } else if model.recipes.isEmpty {
                emptyCatalogue
            } else {
                List(model.recipes, id: \.name, selection: selectedName) { recipe in
                    SourceCatalogueRow(recipe: recipe)
                        .tag(recipe.name)
                }
                .listStyle(.sidebar)
                .scrollContentBackground(.hidden)
            }
        }
        .background(theme.palette.plate)
    }

    private var selectedName: Binding<String?> {
        Binding(
            get: { model.selectedName },
            set: { name in
                guard let name, name != model.selectedName else { return }
                Task {
                    await model.select(name, client: client, appModel: appModel)
                }
            })
    }

    private var emptyCatalogue: some View {
        VStack(alignment: .leading, spacing: .md) {
            Text("No sources yet.")
                .windexStyle(Typography.label)
            Text("A source is a recipe: where to fetch, how to extract, and what to keep.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
            HStack(spacing: .sm) {
                Button("Add a source") {
                    appModel.selection = .recipes
                }
                Button("Browse the marketplace") {
                    appModel.selection = .marketplace
                }
            }
            Spacer()
        }
        .padding(.lg)
    }

    @ViewBuilder
    private var detail: some View {
        if model.isLoadingDetail, model.detail == nil {
            ProgressView()
                .controlSize(.small)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .accessibilityLabel("Loading source detail")
        } else if let recipe = model.detail {
            SourceDetailView(
                recipe: recipe,
                flows: model.flows,
                selectedFlow: Binding(
                    get: { model.selectedFlow },
                    set: { flow in
                        guard let flow, flow != model.selectedFlow else { return }
                        Task {
                            await model.selectFlow(
                                flow,
                                client: client,
                                appModel: appModel)
                        }
                    }),
                tasks: model.tasks,
                executable: model.executable,
                unavailableModules: model.unavailableModules,
                errorMessage: model.errorMessage,
                isRefreshing: model.isLoadingDetail,
                isActing: model.isActing,
                run: {
                    Task {
                        await model.run(client: client, appModel: appModel)
                    }
                })
        } else if let error = model.errorMessage {
            SourceFailureView(message: error) {
                Task {
                    await model.load(client: client, appModel: appModel)
                }
            }
        } else {
            Text("Choose a source.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }
}

private struct SourceCatalogueRow: View {
    let recipe: Recipe
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            HStack(spacing: .xs) {
                Text(recipe.displayTitle)
                    .windexStyle(Typography.label)
                Spacer(minLength: 0)
                if recipe.enabled == false {
                    StatusBadge(.attention, word: "disabled")
                }
            }
            HStack(spacing: .xs) {
                Text(recipe.name)
                Text("·")
                Text(recipe.builtin == true ? "built-in" : "installed")
                if let count = recipe.nodeCount {
                    Text("· \(count) nodes")
                }
            }
            .windexStyle(Typography.dataSM)
            .foregroundStyle(theme.palette.graphite)
        }
        .padding(.vertical, .xxs)
        .accessibilityElement(children: .combine)
    }
}

private struct SourceDetailView: View {
    let recipe: Recipe
    let flows: [String: RecipeFlowSummary]
    @Binding var selectedFlow: String?
    let tasks: [RecipeTask]
    let executable: Bool
    let unavailableModules: [String]
    let errorMessage: String?
    let isRefreshing: Bool
    let isActing: Bool
    let run: () -> Void
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                header
                Hairline()
                flowHeader
                if let flow = selectedFlow, let summary = flows[flow] {
                    RecipeFlowDiagram(summary: summary, tasks: tasks)
                    placementTable
                } else {
                    Text("This recipe has no flows.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                }
                Hairline()
                executionStatus
            }
            .padding(.xl)
            .frame(maxWidth: .infinity, alignment: .leading)
            .opacity(isRefreshing ? 0.6 : 1)
        }
        .background(theme.palette.ink)
    }

    private var executionStatus: some View {
        VStack(alignment: .leading, spacing: .xs) {
            StyledText("Execution", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            if executable {
                Text("This flow is available to run.")
                    .windexStyle(Typography.body)
            } else {
                StatusBadge(.attention, word: "executor migration in progress")
                Text(
                    unavailableModules.isEmpty
                        ? "This server has not published execution availability."
                        : "Unavailable modules: \(unavailableModules.joined(separator: ", "))."
                )
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
                .textSelection(.enabled)
            }
            if let errorMessage {
                Text(errorMessage)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.rust)
            }
        }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: .sm) {
            HStack(alignment: .firstTextBaseline) {
                StyledText(recipe.displayTitle, Typography.setLG)
                Spacer()
                Button("Run now", action: run)
                    .buttonStyle(.borderedProminent)
                    .disabled(!executable || isActing || recipe.enabled == false)
                if recipe.enabled == false {
                    StatusBadge(.attention, word: "disabled")
                } else {
                    StatusBadge(.healthy)
                }
            }
            if let description = recipe.description, !description.isEmpty {
                Text(description)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
                    .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
            }
            HStack(spacing: .sm) {
                metadata(recipe.name)
                metadata(recipe.builtin == true ? "built-in" : "installed")
                if let source = recipe.source { metadata("corpus \(source)") }
                if let version = recipe.version { metadata("version \(version)") }
            }
        }
    }

    private func metadata(_ value: String) -> some View {
        Text(value)
            .windexStyle(Typography.dataSM)
            .foregroundStyle(theme.palette.graphite)
    }

    private var flowHeader: some View {
        HStack {
            StyledText("Flow", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            if flows.count > 1 {
                Picker("Flow", selection: $selectedFlow) {
                    ForEach(flows.keys.sorted(), id: \.self) { flow in
                        Text(flow).tag(Optional(flow))
                    }
                }
                .labelsHidden()
                .pickerStyle(.menu)
            } else if let selectedFlow {
                Text(selectedFlow)
                    .windexStyle(Typography.data)
            }
            Spacer()
            if let selectedFlow, let summary = flows[selectedFlow] {
                Text("\(summary.nodes.count) nodes · \(summary.edges.count) edges")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
        }
    }

    private var placementTable: some View {
        VStack(alignment: .leading, spacing: .xs) {
            StyledText("Task placement", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            ScrollView(.horizontal) {
                Grid(alignment: .leading, horizontalSpacing: 20, verticalSpacing: 0) {
                    GridRow {
                        tableHeader("Node", width: 130)
                        tableHeader("Module", width: 190)
                        tableHeader("Lane", width: 70)
                        tableHeader("Depends on", width: 160)
                        tableHeader("Preconditions", width: 160)
                        tableHeader("Available", width: 80)
                    }
                    ForEach(tasks, id: \.node) { task in
                        Hairline().gridCellColumns(6)
                        GridRow {
                            cell(task.node, width: 130)
                            cell(task.module, width: 190)
                            cell(task.lane, width: 70)
                            cell(task.dependsOn.joined(separator: ", "), width: 160)
                            cell(task.preconditions.joined(separator: ", "), width: 160)
                            cell(task.executable == false ? "no" : "yes", width: 80)
                        }
                        .padding(.vertical, .xs)
                    }
                }
            }
        }
    }

    private func tableHeader(_ value: String, width: CGFloat) -> some View {
        StyledText(value, Typography.eyebrow)
            .foregroundStyle(theme.palette.graphite)
            .frame(width: width, alignment: .leading)
    }

    private func cell(_ value: String, width: CGFloat) -> some View {
        Text(value.isEmpty ? "—" : value)
            .windexStyle(Typography.dataSM)
            .frame(width: width, alignment: .leading)
    }
}

struct RecipeGraphLayout: Equatable, Sendable {
    let nodes: [String]
    let edges: [[String]]
    let levels: [String: Int]
    let rows: [String: Int]
    let levelCount: Int
    let maximumRows: Int

    init(nodes: [String], edges: [[String]]) {
        self.nodes = nodes
        self.edges = edges.filter { $0.count == 2 }

        var depths = Dictionary(uniqueKeysWithValues: nodes.map { ($0, 0) })
        for _ in nodes.indices {
            var changed = false
            for edge in self.edges {
                let candidate = (depths[edge[0]] ?? 0) + 1
                if candidate > (depths[edge[1]] ?? 0) {
                    depths[edge[1]] = candidate
                    changed = true
                }
            }
            if !changed { break }
        }
        levels = depths
        levelCount = (depths.values.max() ?? 0) + 1

        var levelRows: [Int: Int] = [:]
        var computedRows: [String: Int] = [:]
        for node in nodes {
            let level = depths[node] ?? 0
            computedRows[node] = levelRows[level, default: 0]
            levelRows[level, default: 0] += 1
        }
        rows = computedRows
        maximumRows = max(levelRows.values.max() ?? 1, 1)
    }
}

private struct RecipeFlowDiagram: View {
    let summary: RecipeFlowSummary
    let tasks: [RecipeTask]
    @Environment(\.windexTheme) private var theme

    private let nodeSize = CGSize(width: 164, height: 48)
    private let columnGap: CGFloat = 56
    private let rowGap: CGFloat = 20

    var body: some View {
        let layout = RecipeGraphLayout(nodes: summary.nodes, edges: summary.edges)
        let width = CGFloat(layout.levelCount) * nodeSize.width
            + CGFloat(max(layout.levelCount - 1, 0)) * columnGap
        let height = CGFloat(layout.maximumRows) * nodeSize.height
            + CGFloat(max(layout.maximumRows - 1, 0)) * rowGap

        ScrollView(.horizontal) {
            ZStack {
                Canvas { context, _ in
                    for edge in layout.edges {
                        guard let source = position(edge[0], layout: layout),
                              let target = position(edge[1], layout: layout) else {
                            continue
                        }
                        var path = Path()
                        path.move(to: CGPoint(
                            x: source.x + nodeSize.width / 2,
                            y: source.y))
                        path.addCurve(
                            to: CGPoint(
                                x: target.x - nodeSize.width / 2,
                                y: target.y),
                            control1: CGPoint(
                                x: source.x + nodeSize.width / 2 + columnGap / 2,
                                y: source.y),
                            control2: CGPoint(
                                x: target.x - nodeSize.width / 2 - columnGap / 2,
                                y: target.y))
                        context.stroke(
                            path,
                            with: .color(theme.palette.graphite),
                            lineWidth: Layout.hairline)
                    }
                }

                ForEach(Array(layout.nodes.enumerated()), id: \.element) { index, node in
                    graphNode(index: index, node: node)
                        .position(position(node, layout: layout) ?? .zero)
                }
            }
            .frame(width: max(width, 164), height: max(height, 48))
            .padding(.vertical, .xs)
        }
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Recipe flow graph")
    }

    private func graphNode(index: Int, node: String) -> some View {
        let task = tasks.first { $0.node == node }
        return HStack(spacing: .xs) {
            Text(String(format: "%02d", index + 1))
                .foregroundStyle(theme.palette.cyan)
            VStack(alignment: .leading, spacing: 2) {
                Text(node)
                    .lineLimit(1)
                if let task {
                    Text(task.module)
                        .foregroundStyle(theme.palette.graphite)
                        .lineLimit(1)
                }
            }
        }
        .windexStyle(Typography.dataSM)
        .padding(.horizontal, .sm)
        .frame(width: nodeSize.width, height: nodeSize.height, alignment: .leading)
        .background(theme.palette.plate)
        .overlay {
            RoundedRectangle(cornerRadius: Layout.Radius.control)
                .stroke(theme.palette.rule, lineWidth: Layout.hairline)
        }
        .accessibilityElement(children: .combine)
    }

    private func position(
        _ node: String,
        layout: RecipeGraphLayout
    ) -> CGPoint? {
        guard let level = layout.levels[node], let row = layout.rows[node] else {
            return nil
        }
        return CGPoint(
            x: nodeSize.width / 2
                + CGFloat(level) * (nodeSize.width + columnGap),
            y: nodeSize.height / 2
                + CGFloat(row) * (nodeSize.height + rowGap))
    }
}

struct SourceFailureView: View {
    let message: String
    let retry: () -> Void
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .sm) {
            Text(message)
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.rust)
                .textSelection(.enabled)
            Button("Retry", action: retry)
                .buttonStyle(.bordered)
        }
        .padding(.xl)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}
