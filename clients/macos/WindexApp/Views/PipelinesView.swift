import Observation
import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class PipelineComposerModel {
    private struct Snapshot {
        let draft: PipelineDraft
        let selectedFlow: String
        let selectedNodeID: String?
        let positions: [String: CGPoint]
    }

    enum RightPane: String, CaseIterable, Identifiable {
        case modules
        case inspector
        case flow

        var id: Self { self }
        var title: String { rawValue.capitalized }
    }

    var draft: PipelineDraft?
    var selectedCatalogueName: String?
    var selectedFlow = "main"
    var selectedNodeID: String?
    var pendingConnectionSource: String?
    var moduleFilter = ""
    var rightPane: RightPane = .modules
    var positions: [String: CGPoint] = [:]
    var errorMessage: String?
    private(set) var baseVersion: Int?
    private(set) var baseHash: String?
    private(set) var recoveryRevision = 0

    private(set) var issues: [PipelineValidationIssue] = []
    private(set) var nodeForm: FormModel?
    private var undoStack: [Snapshot] = []
    private var redoStack: [Snapshot] = []

    var canUndo: Bool { !undoStack.isEmpty }
    var canRedo: Bool { !redoStack.isEmpty }

    func newPipeline(registry: PipelineRegistry?) {
        draft = PipelineDraft(
            name: "untitled_pipeline",
            title: "Untitled pipeline")
        selectedCatalogueName = nil
        selectedFlow = "main"
        selectedNodeID = nil
        pendingConnectionSource = nil
        positions = [:]
        errorMessage = nil
        baseVersion = nil
        baseHash = nil
        nodeForm = nil
        undoStack = []
        redoStack = []
        validate(registry)
    }

    func renamePipeline(_ title: String) {
        guard var draft, draft.title != title else { return }
        checkpoint()
        draft.title = title
        self.draft = draft
    }

    func open(_ revision: PipelineRevision, registry: PipelineRegistry?) {
        draft = PipelineDraft(revision: revision)
        baseVersion = revision.reference.version
        baseHash = revision.reference.specHash
        selectedCatalogueName = revision.reference.pipeline
        selectedFlow = revision.spec.flows.first?.name ?? "main"
        selectedNodeID = nil
        pendingConnectionSource = nil
        positions = [:]
        nodeForm = nil
        undoStack = []
        redoStack = []
        validate(registry)
    }

    func restore(
        _ recovered: RecoveredPipelineDraft,
        registry: PipelineRegistry?
    ) {
        draft = recovered.draft
        baseVersion = recovered.baseVersion
        baseHash = recovered.baseHash
        selectedCatalogueName = recovered.baseVersion == nil
            ? nil : recovered.draft.name
        selectedFlow = recovered.selectedFlow
        positions = recovered.positions.mapValues {
            CGPoint(x: $0.x, y: $0.y)
        }
        selectedNodeID = nil
        pendingConnectionSource = nil
        nodeForm = nil
        undoStack = []
        redoStack = []
        validate(registry)
    }

    var recoveryRecord: RecoveredPipelineDraft? {
        guard let draft else { return nil }
        return RecoveredPipelineDraft(
            draft: draft,
            baseVersion: baseVersion,
            baseHash: baseHash,
            selectedFlow: selectedFlow,
            positions: positions.mapValues {
                PipelineNodePosition(x: $0.x, y: $0.y)
            })
    }

    func add(_ module: PipelineModuleDescriptor, registry: PipelineRegistry) {
        guard var draft else { return }
        checkpoint()
        do {
            let node = try draft.addNode(module: module, toFlow: selectedFlow)
            self.draft = draft
            selectedNodeID = node.id
            pendingConnectionSource = nil
            let index = currentFlow?.nodes.firstIndex(where: { $0.id == node.id }) ?? 0
            positions[node.id] = Self.defaultPosition(index: index)
            select(node.id, registry: registry)
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func select(_ nodeID: String?, registry: PipelineRegistry?) {
        selectedNodeID = nodeID
        guard let nodeID,
              let node = currentFlow?.nodes.first(where: { $0.id == nodeID }),
              let module = registry?.module(node.module) else {
            nodeForm = nil
            return
        }
        nodeForm = FormModel(
            params: module.fields,
            values: node.config.mapValues(\.wireValue))
    }

    func applyNodeForm(registry: PipelineRegistry) {
        guard let selectedNodeID, let nodeForm,
              var draft,
              let node = currentFlow?.nodes.first(where: { $0.id == selectedNodeID })
        else { return }

        checkpoint()
        let updated = PipelineNode(
            id: node.id,
            kind: node.kind,
            module: node.module,
            config: nodeForm.values.mapValues(NodeConfigValue.init(wireValue:)))
        do {
            try draft.updateNode(updated, inFlow: selectedFlow)
            self.draft = draft
            self.nodeForm = FormModel(
                params: nodeForm.params,
                values: updated.config.mapValues(\.wireValue))
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func removeSelectedNode(registry: PipelineRegistry) {
        guard let selectedNodeID, var draft else { return }
        checkpoint()
        do {
            try draft.removeNode(selectedNodeID, fromFlow: selectedFlow)
            self.draft = draft
            positions.removeValue(forKey: selectedNodeID)
            self.selectedNodeID = nil
            pendingConnectionSource = nil
            nodeForm = nil
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func duplicateSelectedNode(registry: PipelineRegistry) {
        guard let selectedNodeID, var draft else { return }
        checkpoint()
        do {
            let copy = try draft.duplicateNode(selectedNodeID, inFlow: selectedFlow)
            self.draft = draft
            let origin = positions[selectedNodeID] ?? .init(x: 180, y: 120)
            positions[copy.id] = CGPoint(x: origin.x + 32, y: origin.y + 112)
            select(copy.id, registry: registry)
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func renameSelectedNode(
        to value: String,
        registry: PipelineRegistry
    ) {
        guard let selectedNodeID, var draft else { return }
        checkpoint()
        do {
            let previousIDs = Set(
                draft.flows.first { $0.name == selectedFlow }?.nodes.map(\.id) ?? [])
            try draft.renameNode(selectedNodeID, to: value, inFlow: selectedFlow)
            let updatedIDs = Set(
                draft.flows.first { $0.name == selectedFlow }?.nodes.map(\.id) ?? [])
            let renamed = updatedIDs.subtracting(previousIDs).first ?? selectedNodeID
            self.draft = draft
            if renamed != selectedNodeID {
                positions[renamed] = positions.removeValue(forKey: selectedNodeID)
            }
            select(renamed, registry: registry)
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func addFlow() {
        guard var draft else { return }
        checkpoint()
        do {
            var suffix = draft.flows.count + 1
            var name = "flow_\(suffix)"
            while draft.flows.contains(where: { $0.name == name }) {
                suffix += 1
                name = "flow_\(suffix)"
            }
            try draft.addFlow(named: name)
            self.draft = draft
            selectedFlow = name
            selectedNodeID = nil
            nodeForm = nil
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func duplicateFlow() {
        guard var draft else { return }
        checkpoint()
        do {
            var suffix = 2
            var name = "\(selectedFlow)_copy"
            while draft.flows.contains(where: { $0.name == name }) {
                name = "\(selectedFlow)_copy_\(suffix)"
                suffix += 1
            }
            _ = try draft.duplicateFlow(named: selectedFlow, as: name)
            self.draft = draft
            selectedFlow = name
            selectedNodeID = nil
            positions = [:]
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func removeFlow(registry: PipelineRegistry?) {
        guard var draft, draft.flows.count > 1 else { return }
        checkpoint()
        do {
            try draft.removeFlow(named: selectedFlow)
            selectedFlow = draft.flows[0].name
            self.draft = draft
            selectedNodeID = nil
            positions = [:]
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func toggleRefresh(registry: PipelineRegistry?) {
        guard var draft else { return }
        checkpoint()
        do {
            try draft.setRefresh(
                !draft.refreshFlows.contains(selectedFlow),
                forFlow: selectedFlow)
            self.draft = draft
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func addBoundary(
        owner: PipelinePortReference.Owner,
        type: String,
        registry: PipelineRegistry?
    ) {
        guard owner == .input || owner == .output,
              let flow = currentFlow,
              var draft else { return }
        checkpoint()
        var inputs = flow.inputs
        var outputs = flow.outputs
        let existing = Set((owner == .input ? inputs : outputs).map(\.name))
        let base = owner == .input ? "input" : "output"
        var suffix = existing.count + 1
        var name = "\(base)_\(suffix)"
        while existing.contains(name) {
            suffix += 1
            name = "\(base)_\(suffix)"
        }
        let boundary = PipelineBoundary(name: name, title: name, type: type)
        if owner == .input {
            inputs.append(boundary)
        } else {
            outputs.append(boundary)
        }
        do {
            try draft.setBoundaries(
                inputs: inputs,
                outputs: outputs,
                forFlow: selectedFlow)
            self.draft = draft
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func removeBoundary(
        _ boundary: PipelineBoundary,
        owner: PipelinePortReference.Owner,
        registry: PipelineRegistry?
    ) {
        guard let flow = currentFlow, var draft else { return }
        checkpoint()
        let inputs = owner == .input
            ? flow.inputs.filter { $0.name != boundary.name } : flow.inputs
        let outputs = owner == .output
            ? flow.outputs.filter { $0.name != boundary.name } : flow.outputs
        do {
            try draft.setBoundaries(
                inputs: inputs,
                outputs: outputs,
                forFlow: selectedFlow)
            self.draft = draft
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func beginConnection(from nodeID: String) {
        pendingConnectionSource = nodeID
        errorMessage = nil
    }

    func finishConnection(to nodeID: String, registry: PipelineRegistry) {
        guard let source = pendingConnectionSource, source != nodeID,
              var draft else { return }
        checkpoint()
        do {
            try draft.connect(
                PipelineEdge(from: .node(source), to: .node(nodeID)),
                inFlow: selectedFlow,
                registry: registry)
            self.draft = draft
            pendingConnectionSource = nil
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func disconnect(_ edge: PipelineEdge, registry: PipelineRegistry) {
        guard var draft else { return }
        checkpoint()
        do {
            try draft.disconnect(edge, inFlow: selectedFlow)
            self.draft = draft
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func position(for nodeID: String, index: Int) -> CGPoint {
        positions[nodeID] ?? Self.defaultPosition(index: index)
    }

    func move(_ nodeID: String, to position: CGPoint) {
        positions[nodeID] = CGPoint(
            x: max(120, position.x),
            y: max(70, position.y))
        recoveryRevision += 1
    }

    func autoLayout() {
        guard let flow = currentFlow else { return }
        checkpoint()
        positions = Dictionary(
            uniqueKeysWithValues: flow.nodes.enumerated().map {
                ($0.element.id, Self.defaultPosition(index: $0.offset))
            })
    }

    func undo(registry: PipelineRegistry?) {
        guard let current = snapshot, let previous = undoStack.popLast() else { return }
        redoStack.append(current)
        restore(previous, registry: registry)
    }

    func redo(registry: PipelineRegistry?) {
        guard let current = snapshot, let next = redoStack.popLast() else { return }
        undoStack.append(current)
        restore(next, registry: registry)
    }

    func validate(_ registry: PipelineRegistry?) {
        guard let draft, let registry else {
            issues = []
            return
        }
        issues = PipelineLocalValidator.validate(draft, registry: registry)
    }

    var currentFlow: PipelineFlow? {
        draft?.flows.first { $0.name == selectedFlow }
    }

    var errorCount: Int {
        issues.count { $0.severity == .error }
    }

    private static func defaultPosition(index: Int) -> CGPoint {
        CGPoint(
            x: 180 + CGFloat(index % 3) * 280,
            y: 120 + CGFloat(index / 3) * 150)
    }

    private var snapshot: Snapshot? {
        guard let draft else { return nil }
        return Snapshot(
            draft: draft,
            selectedFlow: selectedFlow,
            selectedNodeID: selectedNodeID,
            positions: positions)
    }

    private func checkpoint() {
        guard let snapshot else { return }
        undoStack.append(snapshot)
        if undoStack.count > 100 {
            undoStack.removeFirst(undoStack.count - 100)
        }
        redoStack = []
        recoveryRevision += 1
    }

    private func restore(_ snapshot: Snapshot, registry: PipelineRegistry?) {
        draft = snapshot.draft
        selectedFlow = snapshot.selectedFlow
        positions = snapshot.positions
        select(snapshot.selectedNodeID, registry: registry)
        pendingConnectionSource = nil
        errorMessage = nil
        validate(registry)
        recoveryRevision += 1
    }
}

struct PipelinesView: View {
    @Environment(BackendSession.self) private var session
    @State private var model = PipelineComposerModel()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        HSplitView {
            catalogue
                .frame(minWidth: 210, idealWidth: 240, maxWidth: 290)
            composer
                .frame(minWidth: 520)
            rightPane
                .frame(minWidth: 280, idealWidth: 320, maxWidth: 380)
        }
        .background(theme.palette.ink)
        .onChange(of: session.registry.registry) { _, registry in
            model.validate(registry)
        }
        .task {
            guard model.draft == nil,
                  let recovered = try? await session.draftRecovery.latest()
            else { return }
            model.restore(recovered, registry: session.registry.registry)
        }
        .task(id: model.recoveryRevision) {
            do {
                try await Task.sleep(for: .milliseconds(600))
                guard !Task.isCancelled, let record = model.recoveryRecord else {
                    return
                }
                try await session.draftRecovery.save(record)
            } catch is CancellationError {
                return
            } catch {
                // Recovery is best-effort. Publication remains the durable path.
            }
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack(alignment: .firstTextBaseline) {
                StyledText("Pipelines", Typography.masthead)
                Spacer()
                Button {
                    model.newPipeline(registry: session.registry.registry)
                } label: {
                    Image(systemName: "plus")
                        .accessibilityLabel("New Pipeline")
                }
                .buttonStyle(.plain)
            }
            .padding(.md)
            Hairline()

            if let draft = model.draft, model.selectedCatalogueName == nil {
                PipelineDraftRow(draft: draft)
                    .padding(.horizontal, .md)
                    .padding(.vertical, .sm)
                Hairline()
            }

            if session.pipelines.pipelines.isEmpty {
                VStack(alignment: .leading, spacing: .md) {
                    Text("No saved Pipelines yet.")
                        .windexStyle(Typography.label)
                    Text("Build a typed graph from the backend’s Module registry.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                    Button("New Pipeline") {
                        model.newPipeline(registry: session.registry.registry)
                    }
                    Spacer()
                }
                .padding(.lg)
            } else {
                List(session.pipelines.pipelines, selection: $model.selectedCatalogueName) {
                    pipeline in
                    PipelineSummaryRow(pipeline: pipeline)
                        .tag(pipeline.name)
                }
                .listStyle(.sidebar)
                .scrollContentBackground(.hidden)
                .onChange(of: model.selectedCatalogueName) { _, name in
                    guard let name,
                          let revision = session.pipelines.revisions[name]?.first
                    else { return }
                    model.open(revision, registry: session.registry.registry)
                }
            }
        }
        .background(theme.palette.plate)
    }

    @ViewBuilder
    private var composer: some View {
        if model.draft == nil {
            VStack(alignment: .leading, spacing: .md) {
                StyledText("Visual Pipeline composer", Typography.masthead)
                Text(
                    "Pipelines are reusable, immutable computation graphs. Sources deploy a pinned Pipeline revision with origin and corpus configuration."
                )
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
                Button("Create a Pipeline") {
                    model.newPipeline(registry: session.registry.registry)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(.xl)
        } else {
            VStack(spacing: 0) {
                composerToolbar
                Hairline()
                PipelineCanvas(
                    model: model,
                    registry: session.registry.registry)
            }
        }
    }

    private var composerToolbar: some View {
        HStack(spacing: .sm) {
            if let draft = model.draft {
                TextField(
                    "Pipeline title",
                    text: Binding(
                        get: { draft.title },
                        set: { title in
                            model.renamePipeline(title)
                        }))
                .textFieldStyle(.plain)
                .windexStyle(Typography.label)
                .frame(minWidth: 140, maxWidth: 240)

                Picker("Flow", selection: $model.selectedFlow) {
                    ForEach(draft.flows) { flow in
                        Text(flow.name).tag(flow.name)
                    }
                }
                .labelsHidden()
                .frame(maxWidth: 160)
                .onChange(of: model.selectedFlow) {
                    model.select(nil, registry: session.registry.registry)
                }

                Menu {
                    Button("New Flow") {
                        model.addFlow()
                    }
                    Button("Duplicate Flow") {
                        model.duplicateFlow()
                    }
                    Button(
                        draft.refreshFlows.contains(model.selectedFlow)
                            ? "Remove from refresh"
                            : "Use for refresh"
                    ) {
                        model.toggleRefresh(registry: session.registry.registry)
                    }
                    Divider()
                    Button("Delete Flow", role: .destructive) {
                        model.removeFlow(registry: session.registry.registry)
                    }
                    .disabled(draft.flows.count == 1)
                } label: {
                    Image(systemName: "ellipsis.circle")
                        .accessibilityLabel("Flow actions")
                }
                .menuStyle(.borderlessButton)
                .fixedSize()
            }

            Button {
                model.undo(registry: session.registry.registry)
            } label: {
                Image(systemName: "arrow.uturn.backward")
                    .accessibilityLabel("Undo")
            }
            .buttonStyle(.plain)
            .disabled(!model.canUndo)
            .keyboardShortcut("z", modifiers: .command)

            Button {
                model.redo(registry: session.registry.registry)
            } label: {
                Image(systemName: "arrow.uturn.forward")
                    .accessibilityLabel("Redo")
            }
            .buttonStyle(.plain)
            .disabled(!model.canRedo)
            .keyboardShortcut("z", modifiers: [.command, .shift])

            Button {
                model.autoLayout()
            } label: {
                Image(systemName: "wand.and.stars")
                    .accessibilityLabel("Auto-layout Flow")
            }
            .buttonStyle(.plain)

            Spacer()

            if let source = model.pendingConnectionSource {
                Text("Connecting from \(source)")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.cyan)
                Button("Cancel") {
                    model.pendingConnectionSource = nil
                }
                .buttonStyle(.plain)
            }

            if model.errorCount == 0 {
                StatusBadge(.healthy, word: "valid")
            } else {
                StatusBadge(.fault, word: "\(model.errorCount) errors")
            }

            Button("Save revision") {}
                .disabled(true)
                .help("Revision writes will enable when the canonical Pipeline API lands.")
        }
        .padding(.horizontal, .md)
        .frame(height: 48)
        .background(theme.palette.plate)
    }

    private var rightPane: some View {
        VStack(spacing: 0) {
            Picker("Editor pane", selection: $model.rightPane) {
                ForEach(PipelineComposerModel.RightPane.allCases) { pane in
                    Text(pane.title).tag(pane)
                }
            }
            .pickerStyle(.segmented)
            .padding(.md)
            Hairline()

            switch model.rightPane {
            case .modules:
                ModulePalette(model: model, registry: session.registry.registry)
            case .inspector:
                PipelineNodeInspector(model: model, registry: session.registry.registry)
            case .flow:
                PipelineFlowInspector(model: model, registry: session.registry.registry)
            }
        }
        .background(theme.palette.plate)
    }
}

private struct PipelineDraftRow: View {
    let draft: PipelineDraft
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            Text(draft.title)
                .windexStyle(Typography.label)
            HStack(spacing: .xs) {
                Text(draft.name)
                Text("· local draft")
            }
            .windexStyle(Typography.dataSM)
            .foregroundStyle(theme.palette.graphite)
        }
    }
}

private struct PipelineSummaryRow: View {
    let pipeline: PipelineSummary
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            Text(pipeline.displayTitle)
                .windexStyle(Typography.label)
            Text("\(pipeline.name) @ \(pipeline.headVersion)")
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
        }
        .padding(.vertical, .xxs)
    }
}

private struct PipelineCanvas: View {
    @Bindable var model: PipelineComposerModel
    let registry: PipelineRegistry?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView([.horizontal, .vertical]) {
            ZStack {
                Canvas { context, size in
                    drawGrid(context: &context, size: size)
                    drawEdges(context: &context)
                }

                if let flow = model.currentFlow {
                    ForEach(Array(flow.nodes.enumerated()), id: \.element.id) { index, node in
                        let position = model.position(for: node.id, index: index)
                        PipelineCanvasNode(
                            node: node,
                            kind: registry?.kind(node.kind),
                            module: registry?.module(node.module),
                            position: position,
                            selected: model.selectedNodeID == node.id,
                            connecting: model.pendingConnectionSource == node.id,
                            select: {
                                model.select(node.id, registry: registry)
                                model.rightPane = .inspector
                            },
                            beginConnection: {
                                model.beginConnection(from: node.id)
                            },
                            finishConnection: {
                                guard let registry else { return }
                                model.finishConnection(to: node.id, registry: registry)
                            },
                            move: { model.move(node.id, to: $0) })
                    }
                }
            }
            .frame(width: 1600, height: 1000)
        }
        .background(theme.palette.ink)
    }

    private func drawGrid(context: inout GraphicsContext, size: CGSize) {
        var path = Path()
        stride(from: 0.0, through: size.width, by: 24).forEach { x in
            path.move(to: CGPoint(x: x, y: 0))
            path.addLine(to: CGPoint(x: x, y: size.height))
        }
        stride(from: 0.0, through: size.height, by: 24).forEach { y in
            path.move(to: CGPoint(x: 0, y: y))
            path.addLine(to: CGPoint(x: size.width, y: y))
        }
        context.stroke(path, with: .color(theme.palette.rule.opacity(0.32)), lineWidth: 0.5)
    }

    private func drawEdges(context: inout GraphicsContext) {
        guard let flow = model.currentFlow else { return }
        let indexes = Dictionary(
            uniqueKeysWithValues: flow.nodes.enumerated().map { ($0.element.id, $0.offset) })

        for edge in flow.edges
        where edge.from.owner == .node && edge.to.owner == .node {
            guard let fromIndex = indexes[edge.from.id],
                  let toIndex = indexes[edge.to.id] else { continue }
            let startCenter = model.position(for: edge.from.id, index: fromIndex)
            let endCenter = model.position(for: edge.to.id, index: toIndex)
            let start = CGPoint(x: startCenter.x + 106, y: startCenter.y)
            let end = CGPoint(x: endCenter.x - 106, y: endCenter.y)
            let control = max(60, abs(end.x - start.x) * 0.45)
            var path = Path()
            path.move(to: start)
            path.addCurve(
                to: end,
                control1: CGPoint(x: start.x + control, y: start.y),
                control2: CGPoint(x: end.x - control, y: end.y))
            context.stroke(path, with: .color(theme.palette.cyan), lineWidth: 1.5)
        }
    }
}

private struct PipelineCanvasNode: View {
    let node: PipelineNode
    let kind: PipelineKindDescriptor?
    let module: PipelineModuleDescriptor?
    let position: CGPoint
    let selected: Bool
    let connecting: Bool
    let select: () -> Void
    let beginConnection: () -> Void
    let finishConnection: () -> Void
    let move: (CGPoint) -> Void

    @State private var dragOrigin: CGPoint?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ZStack {
            VStack(alignment: .leading, spacing: .xs) {
                HStack(spacing: .xs) {
                    Text(node.id)
                        .windexStyle(Typography.label)
                    Spacer(minLength: 0)
                    Text(node.kind)
                        .windexStyle(Typography.eyebrow)
                        .foregroundStyle(theme.palette.graphite)
                }
                Text(module?.title ?? node.module)
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(module?.implemented == false
                        ? theme.palette.amber : theme.palette.graphite)
                    .lineLimit(1)
                HStack {
                    Text(kind?.inputType ?? "origin")
                    Spacer()
                    Text(kind?.outputType ?? "sink")
                }
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
            }
            .padding(.sm)
            .frame(width: 200, height: 86)
            .background(theme.palette.plate)
            .overlay {
                Rectangle()
                    .stroke(
                        selected || connecting ? theme.palette.cyan : theme.palette.rule,
                        lineWidth: selected || connecting ? 2 : 1)
            }
            .contentShape(Rectangle())
            .onTapGesture(perform: select)
            .gesture(
                DragGesture()
                    .onChanged { value in
                        let origin = dragOrigin ?? position
                        dragOrigin = origin
                        move(CGPoint(
                            x: origin.x + value.translation.width,
                            y: origin.y + value.translation.height))
                    }
                    .onEnded { _ in dragOrigin = nil })

            if kind?.inputType != nil {
                Button(action: finishConnection) {
                    Circle()
                        .fill(theme.palette.ink)
                        .overlay(Circle().stroke(theme.palette.cyan, lineWidth: 2))
                        .frame(width: 14, height: 14)
                }
                .buttonStyle(.plain)
                .accessibilityLabel("Connect to \(node.id)")
                .offset(x: -100)
            }

            if kind?.outputType != nil {
                Button(action: beginConnection) {
                    Circle()
                        .fill(connecting ? theme.palette.cyan : theme.palette.ink)
                        .overlay(Circle().stroke(theme.palette.cyan, lineWidth: 2))
                        .frame(width: 14, height: 14)
                }
                .buttonStyle(.plain)
                .accessibilityLabel("Connect from \(node.id)")
                .offset(x: 100)
            }
        }
        .position(position)
    }
}

private struct ModulePalette: View {
    @Bindable var model: PipelineComposerModel
    let registry: PipelineRegistry?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(spacing: 0) {
            TextField("Filter Modules", text: $model.moduleFilter)
                .textFieldStyle(.roundedBorder)
                .padding(.md)
            Hairline()

            if let registry {
                List {
                    ForEach(groupedModules(registry), id: \.kind.id) { group in
                        Section(group.kind.title) {
                            ForEach(group.modules) { module in
                                Button {
                                    model.add(module, registry: registry)
                                } label: {
                                    VStack(alignment: .leading, spacing: .xxs) {
                                        HStack {
                                            Text(module.title)
                                                .windexStyle(Typography.label)
                                            Spacer()
                                            if !module.implemented {
                                                StatusBadge(.attention, word: "unavailable")
                                            }
                                        }
                                        Text(module.id)
                                            .windexStyle(Typography.dataSM)
                                            .foregroundStyle(theme.palette.graphite)
                                    }
                                    .contentShape(Rectangle())
                                }
                                .buttonStyle(.plain)
                                .disabled(model.draft == nil)
                            }
                        }
                    }
                }
                .listStyle(.sidebar)
                .scrollContentBackground(.hidden)
            } else if case .failed(let message) = modelRegistryState {
                Text(message)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.rust)
                    .padding(.md)
            } else {
                ProgressView("Loading Module registry…")
                    .controlSize(.small)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
    }

    private var modelRegistryState: StoreLoadState {
        registry == nil ? .loading : .loaded
    }

    private func groupedModules(
        _ registry: PipelineRegistry
    ) -> [(kind: PipelineKindDescriptor, modules: [PipelineModuleDescriptor])] {
        let query = model.moduleFilter.trimmingCharacters(in: .whitespacesAndNewlines)
        return registry.kinds.compactMap { kind in
            let modules = registry.modules.filter { module in
                guard module.kind == kind.id else { return false }
                guard !query.isEmpty else { return true }
                return module.title.localizedCaseInsensitiveContains(query)
                    || module.id.localizedCaseInsensitiveContains(query)
                    || module.summary.localizedCaseInsensitiveContains(query)
            }
            return modules.isEmpty ? nil : (kind, modules)
        }
    }
}

private struct PipelineNodeInspector: View {
    @Bindable var model: PipelineComposerModel
    let registry: PipelineRegistry?
    @State private var nodeName = ""
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                if let node = selectedNode {
                    VStack(alignment: .leading, spacing: .xs) {
                        StyledText(node.id, Typography.masthead)
                        TextField("Node name", text: $nodeName)
                            .textFieldStyle(.roundedBorder)
                            .onSubmit {
                                guard let registry else { return }
                                model.renameSelectedNode(
                                    to: nodeName,
                                    registry: registry)
                            }
                        Text(node.module)
                            .windexStyle(Typography.data)
                            .foregroundStyle(theme.palette.graphite)
                    }

                    if let module = registry?.module(node.module),
                       !module.summary.isEmpty {
                        Text(module.summary)
                            .windexStyle(Typography.body)
                    }

                    if let flow = model.currentFlow {
                        let edges = flow.edges.filter {
                            $0.from.id == node.id || $0.to.id == node.id
                        }
                        if !edges.isEmpty {
                            Hairline()
                            StyledText("Connections", Typography.eyebrow)
                                .foregroundStyle(theme.palette.graphite)
                            ForEach(edges) { edge in
                                HStack(spacing: .xs) {
                                    Text("\(edge.from.id) → \(edge.to.id)")
                                        .windexStyle(Typography.dataSM)
                                    Spacer()
                                    Button {
                                        guard let registry else { return }
                                        model.disconnect(edge, registry: registry)
                                    } label: {
                                        Image(systemName: "xmark")
                                            .accessibilityLabel(
                                                "Disconnect \(edge.from.id) from \(edge.to.id)")
                                    }
                                    .buttonStyle(.plain)
                                }
                            }
                        }
                    }

                    if let form = model.nodeForm, !form.params.isEmpty {
                        Hairline()
                        SchemaForm(model: form)
                        HStack {
                            Button("Apply configuration") {
                                guard let registry else { return }
                                model.applyNodeForm(registry: registry)
                            }
                            .disabled(!form.isDirty || !form.errors.isEmpty)
                            Spacer()
                        }
                    } else {
                        Text("This Module has no configuration fields.")
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.graphite)
                    }

                    Hairline()
                    HStack {
                        Button("Duplicate Node") {
                            guard let registry else { return }
                            model.duplicateSelectedNode(registry: registry)
                        }
                        .keyboardShortcut("d", modifiers: .command)

                        Button("Remove Node", role: .destructive) {
                            guard let registry else { return }
                            model.removeSelectedNode(registry: registry)
                        }
                        .keyboardShortcut(.delete, modifiers: [])
                    }
                } else {
                    StyledText("Inspector", Typography.masthead)
                    Text("Select a Node to edit fields declared by its Module schema.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                }

                if !model.issues.isEmpty {
                    Hairline()
                    StyledText("Validation", Typography.eyebrow)
                        .foregroundStyle(theme.palette.graphite)
                    ForEach(model.issues) { issue in
                        HStack(alignment: .top, spacing: .xs) {
                            StatusBadge(
                                issue.severity == .error ? .fault : .attention,
                                word: issue.severity.rawValue)
                            Text(issue.message)
                                .windexStyle(Typography.body)
                        }
                    }
                }

                if let error = model.errorMessage {
                    Text(error)
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.rust)
                }
            }
            .padding(.md)
        }
        .task(id: model.selectedNodeID) {
            nodeName = model.selectedNodeID ?? ""
        }
    }

    private var selectedNode: PipelineNode? {
        guard let selectedNodeID = model.selectedNodeID else { return nil }
        return model.currentFlow?.nodes.first { $0.id == selectedNodeID }
    }
}

private struct PipelineFlowInspector: View {
    @Bindable var model: PipelineComposerModel
    let registry: PipelineRegistry?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                if let flow = model.currentFlow {
                    VStack(alignment: .leading, spacing: .xs) {
                        StyledText(flow.name, Typography.masthead)
                        Text(
                            "\(flow.nodes.count) Nodes · \(flow.edges.count) Edges"
                        )
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                    }

                    Toggle(
                        "Refresh entry point",
                        isOn: Binding(
                            get: {
                                model.draft?.refreshFlows.contains(flow.name) == true
                            },
                            set: { _ in
                                model.toggleRefresh(registry: registry)
                            }))

                    Hairline()
                    boundaries(
                        "Inputs",
                        values: flow.inputs,
                        owner: .input)
                    Hairline()
                    boundaries(
                        "Outputs",
                        values: flow.outputs,
                        owner: .output)

                    Hairline()
                    StyledText("Search Source rails", Typography.eyebrow)
                        .foregroundStyle(theme.palette.graphite)
                    checklist("External ingress", satisfied: !flow.inputs.isEmpty
                        || flow.nodes.contains {
                            registry?.kind($0.kind)?.inputType == nil
                        })
                    checklist("Stable document path", satisfied: !flow.nodes.isEmpty)
                    checklist("Searchable terminal", satisfied: !flow.outputs.isEmpty
                        || flow.nodes.contains {
                            registry?.kind($0.kind)?.outputType == nil
                        })
                    Text(
                        "Server capability validation is authoritative; local rails remain useful while a draft is incomplete."
                    )
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
                } else {
                    Text("Choose a Flow.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                }
            }
            .padding(.md)
        }
    }

    private func boundaries(
        _ title: String,
        values: [PipelineBoundary],
        owner: PipelinePortReference.Owner
    ) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            HStack {
                StyledText(title, Typography.eyebrow)
                    .foregroundStyle(theme.palette.graphite)
                Spacer()
                Menu {
                    ForEach(registry?.portTypes ?? []) { port in
                        Button(port.title) {
                            model.addBoundary(
                                owner: owner,
                                type: port.name,
                                registry: registry)
                        }
                    }
                } label: {
                    Image(systemName: "plus")
                        .accessibilityLabel("Add \(title.dropLast())")
                }
                .menuStyle(.borderlessButton)
                .fixedSize()
                .disabled(registry?.portTypes.isEmpty != false)
            }

            if values.isEmpty {
                Text("No \(title.lowercased()) declared.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }

            ForEach(values) { boundary in
                HStack {
                    VStack(alignment: .leading, spacing: .xxs) {
                        Text(boundary.displayTitle)
                            .windexStyle(Typography.label)
                        Text(boundary.type)
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.graphite)
                    }
                    Spacer()
                    Button {
                        model.removeBoundary(
                            boundary,
                            owner: owner,
                            registry: registry)
                    } label: {
                        Image(systemName: "xmark")
                            .accessibilityLabel("Remove \(boundary.displayTitle)")
                    }
                    .buttonStyle(.plain)
                }
            }
        }
    }

    private func checklist(_ label: String, satisfied: Bool) -> some View {
        HStack(spacing: .xs) {
            Image(systemName: satisfied ? "checkmark" : "circle")
                .foregroundStyle(
                    satisfied ? theme.palette.graphite : theme.palette.amber)
            Text(label)
                .windexStyle(Typography.body)
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("\(label), \(satisfied ? "satisfied" : "incomplete")")
    }
}
