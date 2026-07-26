import Observation
import SwiftUI
import WindexKit
import WindexUI

enum PipelineRunMode: String, CaseIterable, Identifiable {
    case run
    case dryRun

    var id: Self { self }
    var isDryRun: Bool { self == .dryRun }
    var title: String { self == .dryRun ? "Dry Run" : "Run" }
    var queueTitle: String { self == .dryRun ? "Queue Dry Run" : "Queue Run" }
}

@MainActor
@Observable
final class PipelineCanvasViewport {
    private(set) var zoom: CGFloat = 1

    func setZoom(_ value: CGFloat) {
        zoom = min(2, max(0.5, value))
    }

    func zoomIn() { setZoom(zoom + 0.1) }
    func zoomOut() { setZoom(zoom - 0.1) }

    func translatedPosition(
        origin: CGPoint,
        translation: CGSize
    ) -> CGPoint {
        CGPoint(
            x: origin.x + translation.width / zoom,
            y: origin.y + translation.height / zoom
        )
    }
}

@MainActor
@Observable
final class PipelineComposerModel {
    private struct Presentation {
        let positions: [String: CGPoint]
        let groups: [PipelineLayoutGroup]
        let annotations: [PipelineLayoutAnnotation]
    }
    private struct Snapshot {
        let draft: PipelineDraft
        let selectedFlow: String
        let selectedNodeID: String?
        let positions: [String: CGPoint]
        let groups: [PipelineLayoutGroup]
        let annotations: [PipelineLayoutAnnotation]
    }

    enum RightPane: String, CaseIterable, Identifiable {
        case modules
        case inspector
        case flow
        case parameters

        var id: Self { self }
        var title: String { rawValue.capitalized }
    }

    var draft: PipelineDraft?
    var selectedCatalogueName: String?
    var selectedFlow = "main"
    var selectedNodeID: String?
    var pendingConnectionSource: PipelinePortReference?
    var moduleFilter = ""
    var rightPane: RightPane = .modules
    var focusedFieldKey: String?
    var focusedParameterKey: String?
    var positions: [String: CGPoint] = [:]
    var groups: [PipelineLayoutGroup] = []
    var annotations: [PipelineLayoutAnnotation] = []
    var errorMessage: String?
    private(set) var baseVersion: Int?
    private(set) var baseHash: String?
    private(set) var sourceCapability = PipelineSourceCapability(capable: false)
    private(set) var semanticEditable = false
    private(set) var layoutDirty = false
    private(set) var recoveryRevision = 0
    private(set) var semanticRevision = 0
    private(set) var serverValidationMessage: String?
    private var presentations: [String: Presentation] = [:]
    private var dirtyFlows: Set<String> = []

    private(set) var localIssues: [PipelineValidationIssue] = []
    private(set) var serverIssues: [PipelineValidationIssue] = []
    private(set) var nodeForm: FormModel?
    private var undoStack: [Snapshot] = []
    private var redoStack: [Snapshot] = []

    var canUndo: Bool { !undoStack.isEmpty }
    var canRedo: Bool { !redoStack.isEmpty }
    var issues: [PipelineValidationIssue] {
        var seen = Set<String>()
        return (localIssues + serverIssues).filter { seen.insert($0.id).inserted }
    }

    func newPipeline(registry: PipelineRegistry?) {
        draft = PipelineDraft(
            name: "untitled_pipeline",
            title: "Untitled pipeline")
        selectedCatalogueName = nil
        selectedFlow = "main"
        selectedNodeID = nil
        pendingConnectionSource = nil
        positions = [:]
        groups = []
        annotations = []
        errorMessage = nil
        baseVersion = nil
        baseHash = nil
        sourceCapability = .init(capable: false)
        semanticEditable = true
        layoutDirty = false
        presentations = [:]
        dirtyFlows = []
        nodeForm = nil
        undoStack = []
        redoStack = []
        semanticRevision += 1
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
        sourceCapability = revision.sourceCapability
        semanticEditable = false
        layoutDirty = false
        presentations = [:]
        dirtyFlows = []
        selectedCatalogueName = revision.reference.pipeline
        selectedFlow = revision.spec.flows.first?.name ?? "main"
        selectedNodeID = nil
        pendingConnectionSource = nil
        positions = [:]
        groups = []
        annotations = []
        nodeForm = nil
        undoStack = []
        redoStack = []
        serverIssues = []
        serverValidationMessage = nil
        validate(registry)
    }

    func apply(_ layout: PipelineFlowLayout?) {
        if let cached = presentations[selectedFlow] {
            positions = cached.positions
            groups = cached.groups
            annotations = cached.annotations
            layoutDirty = dirtyFlows.contains(selectedFlow)
            return
        }
        positions = layout?.positions.mapValues {
            CGPoint(x: $0.x, y: $0.y)
        } ?? [:]
        groups = layout?.groups ?? []
        annotations = layout?.annotations ?? []
        layoutDirty = false
        presentations[selectedFlow] = Presentation(
            positions: positions,
            groups: groups,
            annotations: annotations
        )
    }

    func accept(_ layout: PipelineFlowLayout?) {
        presentations.removeValue(forKey: selectedFlow)
        dirtyFlows.remove(selectedFlow)
        apply(layout)
    }

    func stashLayout(for flow: String) {
        presentations[flow] = Presentation(
            positions: positions,
            groups: groups,
            annotations: annotations
        )
        if layoutDirty { dirtyFlows.insert(flow) }
    }

    func presentation(
        for flow: String,
        fallback: PipelineFlowLayout?
    ) -> (
        positions: [String: PipelineNodePosition],
        groups: [PipelineLayoutGroup],
        annotations: [PipelineLayoutAnnotation]
    ) {
        let value = presentations[flow]
        return (
            value?.positions.mapValues {
                PipelineNodePosition(x: $0.x, y: $0.y)
            } ?? fallback?.positions ?? [:],
            value?.groups ?? fallback?.groups ?? [],
            value?.annotations ?? fallback?.annotations ?? []
        )
    }

    func beginNewRevision() {
        guard baseVersion != nil else { return }
        semanticEditable = true
        selectedNodeID = nil
        nodeForm = nil
        undoStack = []
        redoStack = []
        recoveryRevision += 1
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
        groups = recovered.groups ?? []
        annotations = recovered.annotations ?? []
        semanticEditable = true
        layoutDirty = false
        presentations = [
            recovered.selectedFlow: Presentation(
                positions: positions,
                groups: groups,
                annotations: annotations
            ),
        ]
        dirtyFlows = [recovered.selectedFlow]
        selectedNodeID = nil
        pendingConnectionSource = nil
        nodeForm = nil
        undoStack = []
        redoStack = []
        semanticRevision += 1
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
            },
            groups: groups,
            annotations: annotations)
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
            values: Dictionary(uniqueKeysWithValues: module.fields.compactMap { field in
                if case .literal(let value)? = node.config[field.key] {
                    return (field.key, value)
                }
                return field.initialValue.map { (field.key, $0) }
            }))
    }

    func applyNodeForm(registry: PipelineRegistry) {
        guard let selectedNodeID, let nodeForm,
              var draft,
              let node = currentFlow?.nodes.first(where: { $0.id == selectedNodeID })
        else { return }

        checkpoint()
        var config = node.config
        for (key, value) in nodeForm.values {
            if config[key]?.bindingMode == .pipelineParameter
                || config[key]?.bindingMode == .secretReference {
                continue
            }
            config[key] = .literal(value)
        }
        let updated = PipelineNode(
            id: node.id,
            kind: node.kind,
            module: node.module,
            config: config)
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

    func setNodeBinding(
        field key: String,
        value: NodeConfigValue,
        registry: PipelineRegistry
    ) {
        guard let selectedNodeID, var draft,
              let node = currentFlow?.nodes.first(where: { $0.id == selectedNodeID })
        else { return }
        checkpoint()
        var config = node.config
        config[key] = value
        do {
            try draft.updateNode(
                PipelineNode(
                    id: node.id,
                    kind: node.kind,
                    module: node.module,
                    config: config
                ),
                inFlow: selectedFlow
            )
            self.draft = draft
            select(selectedNodeID, registry: registry)
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func addParameter(
        _ definition: PipelineParameterDefinition,
        registry: PipelineRegistry?
    ) {
        guard var draft else { return }
        checkpoint()
        do {
            try draft.addParameter(definition)
            self.draft = draft
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func updateParameter(
        previousKey: String,
        definition: PipelineParameterDefinition,
        registry: PipelineRegistry?
    ) {
        guard var draft else { return }
        checkpoint()
        do {
            try draft.updateParameter(named: previousKey, definition: definition)
            self.draft = draft
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func removeParameter(_ key: String, registry: PipelineRegistry?) {
        guard var draft else { return }
        checkpoint()
        draft.removeParameter(named: key)
        self.draft = draft
        validate(registry)
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

    func addFlow(registry: PipelineRegistry? = nil) {
        guard var draft else { return }
        stashLayout(for: selectedFlow)
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
            positions = [:]
            groups = []
            annotations = []
            layoutDirty = false
            presentations[name] = Presentation(
                positions: positions,
                groups: groups,
                annotations: annotations
            )
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func duplicateFlow(registry: PipelineRegistry? = nil) {
        guard var draft else { return }
        stashLayout(for: selectedFlow)
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
            layoutDirty = true
            dirtyFlows.insert(name)
            presentations[name] = Presentation(
                positions: positions,
                groups: groups,
                annotations: annotations
            )
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func renameSelectedFlow(to rawName: String, registry: PipelineRegistry?) {
        guard var draft,
              let flowIndex = draft.flows.firstIndex(where: { $0.name == selectedFlow })
        else { return }
        let previous = selectedFlow
        checkpoint()
        do {
            stashLayout(for: previous)
            try draft.renameFlow(named: previous, to: rawName)
            let renamed = draft.flows[flowIndex].name
            let presentation = presentations.removeValue(forKey: previous)
            let wasDirty = dirtyFlows.remove(previous) != nil
            if let presentation { presentations[renamed] = presentation }
            if wasDirty { dirtyFlows.insert(renamed) }
            self.draft = draft
            selectedFlow = renamed
            validate(registry)
        } catch {
            undoStack.removeLast()
            errorMessage = error.localizedDescription
        }
    }

    func removeFlow(registry: PipelineRegistry?) {
        guard var draft, draft.flows.count > 1 else { return }
        let removedFlow = selectedFlow
        checkpoint()
        do {
            try draft.removeFlow(named: removedFlow)
            presentations.removeValue(forKey: removedFlow)
            dirtyFlows.remove(removedFlow)
            selectedFlow = draft.flows[0].name
            self.draft = draft
            selectedNodeID = nil
            apply(nil)
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

    func beginConnection(from source: PipelinePortReference) {
        pendingConnectionSource = source
        errorMessage = nil
    }

    func finishConnection(
        to target: PipelinePortReference,
        registry: PipelineRegistry
    ) {
        guard let source = pendingConnectionSource,
              source != target,
              canConnect(to: target, registry: registry),
              var draft else { return }
        checkpoint()
        do {
            try draft.connect(
                PipelineEdge(from: source, to: target),
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

    func canConnect(
        to target: PipelinePortReference,
        registry: PipelineRegistry
    ) -> Bool {
        guard let source = pendingConnectionSource,
              source != target,
              var candidate = draft else { return false }
        return (try? candidate.connect(
            PipelineEdge(from: source, to: target),
            inFlow: selectedFlow,
            registry: registry
        )) != nil
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
        markLayoutDirty()
        recoveryRevision += 1
    }

    func autoLayout() {
        guard let flow = currentFlow else { return }
        checkpoint()
        positions = Dictionary(
            uniqueKeysWithValues: flow.nodes.enumerated().map {
                ($0.element.id, Self.defaultPosition(index: $0.offset))
            })
        markLayoutDirty()
    }

    func groupSelectedNode() {
        guard let selectedNodeID,
              let index = currentFlow?.nodes.firstIndex(where: { $0.id == selectedNodeID })
        else { return }
        let center = position(for: selectedNodeID, index: index)
        groups.append(
            PipelineLayoutGroup(
                id: "group-\(groups.count + 1)",
                title: "Group \(groups.count + 1)",
                nodes: [selectedNodeID],
                x: center.x - 130,
                y: center.y - 70
            )
        )
        markLayoutDirty()
        recoveryRevision += 1
    }

    func updateGroup(_ group: PipelineLayoutGroup) {
        guard let index = groups.firstIndex(where: { $0.id == group.id }) else { return }
        groups[index] = group
        markLayoutDirty()
        recoveryRevision += 1
    }

    func removeGroup(_ id: String) {
        groups.removeAll { $0.id == id }
        markLayoutDirty()
        recoveryRevision += 1
    }

    func addAnnotation() {
        annotations.append(
            PipelineLayoutAnnotation(
                id: "annotation-\(annotations.count + 1)",
                text: "Annotation",
                x: 160 + Double(annotations.count * 28),
                y: 80 + Double(annotations.count * 28)
            )
        )
        markLayoutDirty()
        recoveryRevision += 1
    }

    func updateAnnotation(_ annotation: PipelineLayoutAnnotation) {
        guard let index = annotations.firstIndex(where: { $0.id == annotation.id })
        else { return }
        annotations[index] = annotation
        markLayoutDirty()
        recoveryRevision += 1
    }

    func removeAnnotation(_ id: String) {
        annotations.removeAll { $0.id == id }
        markLayoutDirty()
        recoveryRevision += 1
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
            localIssues = []
            return
        }
        localIssues = PipelineLocalValidator.validate(draft, registry: registry)
    }

    func applyServerValidation(
        _ validation: PipelineValidationWire,
        revision: Int
    ) {
        guard revision == semanticRevision else { return }
        serverIssues = validation.issues.map {
            PipelineValidationIssue(
                path: $0.path,
                code: $0.code,
                severity: $0.severity == .error ? .error : .warning,
                message: $0.message
            )
        }
        serverValidationMessage = nil
    }

    func failServerValidation(_ error: Error, revision: Int) {
        guard revision == semanticRevision else { return }
        serverValidationMessage = error.localizedDescription
    }

    @discardableResult
    func focus(
        _ issue: PipelineValidationIssue,
        registry: PipelineRegistry?
    ) -> String? {
        guard let draft else { return nil }
        focusedFieldKey = nil
        focusedParameterKey = nil
        let parts = issue.path.split(separator: ".").map(String.init)
        guard let flowMarker = parts.firstIndex(of: "flows"),
              parts.indices.contains(flowMarker + 1) else {
            if let parameterMarker = parts.firstIndex(of: "parameters"),
               parts.indices.contains(parameterMarker + 1) {
                focusedParameterKey = parts[parameterMarker + 1]
                rightPane = .parameters
            }
            return selectedFlow
        }
        let flowName = parts[flowMarker + 1]
        guard draft.flows.contains(where: { $0.name == flowName }) else {
            return selectedFlow
        }
        if flowName != selectedFlow {
            stashLayout(for: selectedFlow)
            selectedFlow = flowName
        }
        if let nodeMarker = parts.firstIndex(of: "nodes"),
           parts.indices.contains(nodeMarker + 1) {
            let nodeID = parts[nodeMarker + 1]
            select(nodeID, registry: registry)
            rightPane = .inspector
            if let fieldMarker = parts.firstIndex(where: {
                $0 == "with" || $0 == "config"
            }), parts.indices.contains(fieldMarker + 1) {
                focusedFieldKey = parts[fieldMarker + 1]
            }
        } else {
            select(nil, registry: registry)
            rightPane = .flow
        }
        return flowName
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
            positions: positions,
            groups: groups,
            annotations: annotations)
    }

    private func checkpoint() {
        guard let snapshot else { return }
        undoStack.append(snapshot)
        if undoStack.count > 100 {
            undoStack.removeFirst(undoStack.count - 100)
        }
        redoStack = []
        recoveryRevision += 1
        semanticRevision += 1
        serverIssues = []
        serverValidationMessage = nil
    }

    private func markLayoutDirty() {
        layoutDirty = true
        dirtyFlows.insert(selectedFlow)
        presentations[selectedFlow] = Presentation(
            positions: positions,
            groups: groups,
            annotations: annotations
        )
    }

    private func restore(_ snapshot: Snapshot, registry: PipelineRegistry?) {
        draft = snapshot.draft
        selectedFlow = snapshot.selectedFlow
        positions = snapshot.positions
        groups = snapshot.groups
        annotations = snapshot.annotations
        markLayoutDirty()
        select(snapshot.selectedNodeID, registry: registry)
        pendingConnectionSource = nil
        errorMessage = nil
        validate(registry)
        recoveryRevision += 1
        semanticRevision += 1
        serverIssues = []
        serverValidationMessage = nil
    }
}

struct PipelinesView: View {
    @Bindable var appModel: AppModel
    @Environment(BackendSession.self) private var session
    @State private var model = PipelineComposerModel()
    @State private var isSaving = false
    @State private var isPresentingRun = false
    @State private var isArchiving = false
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
        .task(id: appModel.pipelineNavigation) {
            await openNavigationRequest()
        }
        .onChange(of: session.pipelines.revisions) {
            Task { await openNavigationRequest() }
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
        .task(id: model.semanticRevision) {
            guard model.semanticEditable,
                  let spec = model.draft?.spec else { return }
            let revision = model.semanticRevision
            do {
                try await Task.sleep(for: .milliseconds(450))
                guard !Task.isCancelled else { return }
                let validation = try await session.client.validatePipeline(spec)
                model.applyServerValidation(validation, revision: revision)
            } catch is CancellationError {
                return
            } catch {
                model.failServerValidation(error, revision: revision)
            }
        }
        .sheet(isPresented: $isPresentingRun) {
            if let draft = model.draft,
               let version = model.baseVersion,
               let revision = session.pipelines.revisions[draft.name]?
                .first(where: { $0.reference.version == version }) {
                PipelineRunSheet(
                    appModel: appModel,
                    revision: revision,
                    selectedFlow: model.selectedFlow
                )
                .frame(minWidth: 560, minHeight: 520)
            }
        }
    }

    private func openNavigationRequest() async {
        guard let request = appModel.pipelineNavigation,
              let revision = session.pipelines.revisions[request.reference.pipeline]?
                .first(where: { $0.reference.version == request.reference.version })
        else { return }
        model.open(revision, registry: session.registry.registry)
        if let flow = request.flow,
           revision.spec.flows.contains(where: { $0.name == flow }) {
            model.selectedFlow = flow
        }
        await session.loadLayout(
            pipeline: revision.reference.pipeline,
            version: revision.reference.version,
            flow: model.selectedFlow
        )
        model.apply(
            session.pipelines.layout(
                pipeline: revision.reference.pipeline,
                version: revision.reference.version,
                flow: model.selectedFlow
            )
        )
        appModel.pipelineNavigation = nil
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
                    model.apply(session.pipelines.layout(
                        pipeline: name,
                        version: revision.reference.version,
                        flow: model.selectedFlow
                    ))
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

    private var selectedGenericRunCapability: PipelineGenericRunCapability {
        guard let registry = session.registry.registry else { return .unavailable }
        return registry.genericRunCapability(for: model.currentFlow)
    }

    private var matchingSources: [SourceDeployment] {
        guard let draft = model.draft, let version = model.baseVersion else {
            return []
        }
        return session.sources.sources.filter {
            !$0.archived
                && $0.pipeline.pipeline == draft.name
                && $0.pipeline.version == version
        }
    }

    @ViewBuilder
    private var sourceDestinationControl: some View {
        if matchingSources.count == 1, let source = matchingSources.first {
            Button("Open Source") {
                appModel.openSource(source.name)
            }
            .help(
                "Open \(source.displayTitle) and use Run latest with its Source binding."
            )
        } else if !matchingSources.isEmpty {
            Menu("Open Source") {
                ForEach(matchingSources) { source in
                    Button(source.displayTitle) {
                        appModel.openSource(source.name)
                    }
                }
            }
            .help("Choose a Source deployment to run.")
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
                .disabled(!model.semanticEditable)

                Picker(
                    "Flow",
                    selection: Binding(
                        get: { model.selectedFlow },
                        set: { flow in
                            let previous = model.selectedFlow
                            guard flow != previous else { return }
                            model.stashLayout(for: previous)
                            model.selectedFlow = flow
                            model.select(nil, registry: session.registry.registry)
                            guard let draft = model.draft,
                                  let version = model.baseVersion
                            else {
                                model.apply(nil)
                                return
                            }
                            model.apply(session.pipelines.layout(
                                pipeline: draft.name,
                                version: version,
                                flow: flow
                            ))
                        }
                    )
                ) {
                    ForEach(draft.flows) { flow in
                        Text(flow.name).tag(flow.name)
                    }
                }
                .labelsHidden()
                .frame(maxWidth: 160)

                if let version = model.baseVersion {
                    Menu("v\(version)") {
                        ForEach(
                            session.pipelines.revisions[draft.name] ?? [],
                            id: \.reference
                        ) { revision in
                            Button {
                                Task { await openRevision(revision) }
                            } label: {
                                Text(
                                    "v\(revision.reference.version)"
                                        + (revision.note.isEmpty ? "" : " · \(revision.note)")
                                )
                            }
                        }
                    }
                    .help("Open an immutable revision from Pipeline history.")
                }

                Menu {
                    Button("New Flow") {
                        model.addFlow(registry: session.registry.registry)
                    }
                    Button("Duplicate Flow") {
                        model.duplicateFlow(registry: session.registry.registry)
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
                .disabled(!model.semanticEditable)
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
                Text("Connecting from \(source.owner.rawValue):\(source.id)")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.cyan)
                Button("Cancel") {
                    model.pendingConnectionSource = nil
                }
                .buttonStyle(.plain)
            }

            if model.issues.isEmpty {
                StatusBadge(.healthy, word: "valid")
            } else {
                Menu {
                    ForEach(model.issues) { issue in
                        Button {
                            focusDiagnostic(issue)
                        } label: {
                            Label(
                                issue.message,
                                systemImage: issue.severity == .error
                                    ? "exclamationmark.circle"
                                    : "exclamationmark.triangle"
                            )
                        }
                    }
                } label: {
                    StatusBadge(
                        model.errorCount == 0 ? .attention : .fault,
                        word: model.errorCount == 0
                            ? "\(model.issues.count) warnings"
                            : "\(model.errorCount) errors"
                    )
                }
                .menuStyle(.borderlessButton)
                .help("Choose a diagnostic to focus its Flow, Node, or field.")
            }

            if model.baseVersion != nil, !model.semanticEditable {
                Button("Run") {
                    isPresentingRun = true
                }
                .disabled(!selectedGenericRunCapability.canRun)
                .help(
                    selectedGenericRunCapability.canRun
                        ? "Run this explicit immutable Pipeline revision."
                        : selectedGenericRunCapability.explanation
                )
                if selectedGenericRunCapability.requiresSource {
                    StatusBadge(.attention, word: "Source required")
                        .help(selectedGenericRunCapability.explanation)
                    sourceDestinationControl
                }
            }

            if model.semanticEditable {
                Button(isSaving ? "Publishing…" : "Publish revision") {
                    Task { await saveRevision() }
                }
                .disabled(isSaving || model.errorCount > 0 || model.draft == nil)
            } else if let draft = model.draft, let version = model.baseVersion {
                Button("New revision") {
                    model.beginNewRevision()
                }
                Button(isSaving ? "Saving…" : "Save layout") {
                    Task { await saveCurrentLayout() }
                }
                .disabled(isSaving || !model.layoutDirty)
                if model.sourceCapability.capable && matchingSources.isEmpty {
                    Button("Use as Source") {
                        appModel.createSource(
                            using: .init(
                                pipeline: draft.name,
                                version: version,
                                specHash: model.baseHash ?? ""
                            )
                        )
                    }
                } else {
                    Button("Use as Source") {}
                        .disabled(true)
                        .help(
                            model.sourceCapability.issues.first?.message
                                ?? "This revision does not satisfy the Search Source capability."
                        )
                }
            }

            if model.baseVersion != nil, !model.semanticEditable {
                Menu {
                    Button("Archive Pipeline", role: .destructive) {
                        Task { await archiveCurrentPipeline() }
                    }
                    .disabled(isArchiving)
                } label: {
                    Image(systemName: "ellipsis.circle")
                        .accessibilityLabel("Pipeline actions")
                }
                .menuStyle(.borderlessButton)
            }
        }
        .padding(.horizontal, .md)
        .frame(height: 48)
        .background(theme.palette.plate)
    }

    private func openRevision(_ revision: PipelineRevision) async {
        model.open(revision, registry: session.registry.registry)
        await session.loadLayout(
            pipeline: revision.reference.pipeline,
            version: revision.reference.version,
            flow: model.selectedFlow
        )
        model.apply(session.pipelines.layout(
            pipeline: revision.reference.pipeline,
            version: revision.reference.version,
            flow: model.selectedFlow
        ))
    }

    private func focusDiagnostic(_ issue: PipelineValidationIssue) {
        let previousFlow = model.selectedFlow
        guard let flow = model.focus(
            issue,
            registry: session.registry.registry
        ), flow != previousFlow,
           let draft = model.draft,
           let version = model.baseVersion else { return }
        model.apply(session.pipelines.layout(
            pipeline: draft.name,
            version: version,
            flow: flow
        ))
    }

    private func archiveCurrentPipeline() async {
        guard let name = model.draft?.name else { return }
        isArchiving = true
        defer { isArchiving = false }
        do {
            try await session.archivePipeline(name)
            model.newPipeline(registry: session.registry.registry)
        } catch {
            model.errorMessage = error.localizedDescription
        }
    }

    private func saveRevision() async {
        guard let draft = model.draft else { return }
        isSaving = true
        defer { isSaving = false }
        do {
            let savedFlow = model.selectedFlow
            let parent = model.baseVersion.flatMap { version in
                session.pipelines.revisions[draft.name]?.first {
                    $0.reference.version == version
                }
            }
            model.stashLayout(for: savedFlow)
            let presentations = Dictionary(
                uniqueKeysWithValues: draft.flows.map { flow in
                    let parentLayout = parent.flatMap {
                        session.pipelines.layout(
                            pipeline: draft.name,
                            version: $0.reference.version,
                            flow: flow.name
                        )
                    }
                    return (
                        flow.name,
                        model.presentation(for: flow.name, fallback: parentLayout)
                    )
                }
            )
            try await session.publish(draft: draft, parent: parent)
            guard let published = session.pipelines.revisions[draft.name]?.first else { return }
            var layouts: [PipelineFlowLayout] = []
            for flow in published.spec.flows {
                guard var layout = session.pipelines.layout(
                    pipeline: draft.name,
                    version: published.reference.version,
                    flow: flow.name
                ), let presentation = presentations[flow.name]
                else { continue }
                let nodeIDs = Set(flow.nodes.map(\.id))
                layout.positions.merge(
                    presentation.positions.filter { nodeIDs.contains($0.key) }
                ) { _, saved in saved }
                layout.positions = layout.positions.filter { nodeIDs.contains($0.key) }
                layout.groups = presentation.groups.compactMap { original in
                    var group = original
                    if group.fields["nodes"] != nil {
                        group.nodes = group.nodes.filter(nodeIDs.contains)
                        guard !group.nodes.isEmpty else { return nil }
                    }
                    return group
                }
                layout.annotations = presentation.annotations
                layouts.append(layout)
            }
            try await session.saveLayouts(layouts)
            model.open(published, registry: session.registry.registry)
            if published.spec.flows.contains(where: { $0.name == savedFlow }) {
                model.selectedFlow = savedFlow
            }
            model.accept(session.pipelines.layout(
                pipeline: draft.name,
                version: published.reference.version,
                flow: model.selectedFlow
            ))
        } catch {
            model.errorMessage = error.localizedDescription
        }
    }

    private func saveCurrentLayout() async {
        guard let draft = model.draft, let version = model.baseVersion,
              var layout = session.pipelines.layout(
                pipeline: draft.name,
                version: version,
                flow: model.selectedFlow
              )
        else {
            model.errorMessage = "The layout ETag is unavailable. Reload this revision."
            return
        }
        isSaving = true
        defer { isSaving = false }
        do {
            layout.positions = model.positions.mapValues {
                PipelineNodePosition(x: $0.x, y: $0.y)
            }
            layout.groups = model.groups
            layout.annotations = model.annotations
            try await session.saveLayout(layout)
            model.accept(
                session.pipelines.layout(
                    pipeline: draft.name,
                    version: version,
                    flow: model.selectedFlow
                )
            )
        } catch {
            model.errorMessage = error.localizedDescription
        }
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
            case .parameters:
                PipelineParameterInspector(model: model, registry: session.registry.registry)
            }
        }
        .background(theme.palette.plate)
    }
}

private struct PipelineRunSheet: View {
    let appModel: AppModel
    let revision: PipelineRevision
    @State var selectedFlow: String

    @Environment(BackendSession.self) private var session
    @Environment(\.dismiss) private var dismiss
    @Environment(\.windexTheme) private var theme
    @State private var inputValues: [String: String]
    @State private var parameterForm: FormModel
    @State private var priority = 50.0
    @State private var mode: PipelineRunMode = .run
    @State private var isRunning = false
    @State private var errorMessage: String?

    init(
        appModel: AppModel,
        revision: PipelineRevision,
        selectedFlow: String
    ) {
        self.appModel = appModel
        self.revision = revision
        _selectedFlow = State(initialValue: selectedFlow)
        _inputValues = State(
            initialValue: Dictionary(
                uniqueKeysWithValues: (
                    revision.spec.flows.first { $0.name == selectedFlow }?.inputs ?? []
                ).map { ($0.name, "") }
            )
        )
        _parameterForm = State(
            initialValue: FormModel(params: revision.spec.parameters)
        )
    }

    private var currentFlow: PipelineFlow? {
        revision.spec.flows.first { $0.name == selectedFlow }
    }

    private var genericRunCapability: PipelineGenericRunCapability {
        guard let registry = session.registry.registry else { return .unavailable }
        return registry.genericRunCapability(for: currentFlow)
    }

    private var matchingSources: [SourceDeployment] {
        session.sources.sources.filter {
            !$0.archived
                && $0.pipeline.pipeline == revision.reference.pipeline
                && $0.pipeline.version == revision.reference.version
        }
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                VStack(alignment: .leading, spacing: .xs) {
                    StyledText("Run Pipeline", Typography.setLG)
                    Text(
                        "\(revision.reference.pipeline) @ \(revision.reference.version)"
                    )
                        .windexStyle(Typography.data)
                        .foregroundStyle(theme.palette.graphite)
                }
                Spacer()
                Button("Cancel") { dismiss() }
            }
            .padding(.lg)
            Hairline()

            ScrollView {
                VStack(alignment: .leading, spacing: .lg) {
                    Text(
                        "This queues the selected immutable revision. It is distinct from running a Source, which adds Source origin and configuration."
                    )
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)

                    Picker("Execution", selection: $mode) {
                        ForEach(PipelineRunMode.allCases) { mode in
                            Text(mode.title).tag(mode)
                        }
                    }
                    .pickerStyle(.segmented)

                    Text(
                        mode.isDryRun
                            ? "Dry Run validates and plans the frozen revision without committing its normal output side effects. The queued item is still a regular Run with live status and detail."
                            : "Run executes the frozen revision normally."
                    )
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)

                    Picker("Flow", selection: $selectedFlow) {
                        ForEach(revision.spec.flows, id: \.name) {
                            Text($0.name).tag($0.name)
                        }
                    }
                    .onChange(of: selectedFlow) {
                        inputValues = Dictionary(
                            uniqueKeysWithValues: (currentFlow?.inputs ?? []).map {
                                ($0.name, "")
                            }
                        )
                        errorMessage = nil
                    }

                    if !genericRunCapability.canRun {
                        VStack(alignment: .leading, spacing: .sm) {
                            StatusBadge(
                                genericRunCapability.requiresSource
                                    ? .attention
                                    : .fault,
                                word: genericRunCapability.requiresSource
                                    ? "Source required"
                                    : "Run unavailable"
                            )
                            Text(genericRunCapability.explanation)
                                .windexStyle(Typography.body)
                                .foregroundStyle(theme.palette.graphite)

                            ForEach(genericRunCapability.blockers) { blocker in
                                Text(
                                    "\(blocker.moduleID) · "
                                        + blocker.roles.joined(separator: ", ")
                                )
                                .windexStyle(Typography.dataSM)
                                .textSelection(.enabled)
                            }

                            if matchingSources.count == 1,
                               let source = matchingSources.first {
                                Button("Open \(source.displayTitle)") {
                                    dismiss()
                                    appModel.openSource(source.name)
                                }
                            } else if !matchingSources.isEmpty {
                                Menu("Open Source") {
                                    ForEach(matchingSources) { source in
                                        Button(source.displayTitle) {
                                            dismiss()
                                            appModel.openSource(source.name)
                                        }
                                    }
                                }
                            } else if revision.sourceCapability.capable {
                                Button("Use as Source") {
                                    dismiss()
                                    appModel.createSource(using: revision.reference)
                                }
                            }
                        }
                        .padding(.md)
                        .background(theme.palette.plate)
                    }

                    StyledText("Explicit inputs", Typography.eyebrow)
                        .foregroundStyle(theme.palette.graphite)
                    if currentFlow?.inputs.isEmpty != false {
                        Text("This Flow declares no boundary inputs.")
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.graphite)
                    }
                    ForEach(currentFlow?.inputs ?? []) { input in
                        jsonEditor(
                            input.displayTitle,
                            help: "\(input.name) · nominal type \(input.type)",
                            text: Binding(
                                get: { inputValues[input.name] ?? "" },
                                set: { inputValues[input.name] = $0 }
                            )
                        )
                    }

                    StyledText("Pipeline parameters", Typography.eyebrow)
                        .foregroundStyle(theme.palette.graphite)
                    if revision.spec.parameters.isEmpty {
                        Text("This revision declares no Pipeline parameters.")
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.graphite)
                    } else {
                        SchemaForm(
                            model: parameterForm,
                            configuredSecretReferences: session.sources.configuredSecrets
                        )
                    }

                    HStack {
                        Text("Priority \(Int(priority))")
                            .windexStyle(Typography.label)
                        Slider(value: $priority, in: 0...100, step: 1)
                    }

                    if let errorMessage {
                        Text(errorMessage)
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.rust)
                            .textSelection(.enabled)
                    }
                }
                .padding(.xl)
            }

            Hairline()
            HStack {
                Spacer()
                Button(isRunning ? "Queuing…" : mode.queueTitle) {
                    Task { await queue() }
                }
                .buttonStyle(.borderedProminent)
                .disabled(
                    isRunning || selectedFlow.isEmpty
                        || !genericRunCapability.canRun
                        || !parameterForm.errors.isEmpty
                        || inputValues.values.contains {
                            $0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                        }
                )
            }
            .padding(.lg)
        }
        .background(theme.palette.ink)
    }

    private func jsonEditor(
        _ title: String,
        help: String,
        text: Binding<String>
    ) -> some View {
        VStack(alignment: .leading, spacing: .xs) {
            Text(title).windexStyle(Typography.label)
            Text(help)
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
            TextEditor(text: text)
                .font(.system(.body, design: .monospaced))
                .frame(minHeight: 100)
                .padding(.xs)
                .background(theme.palette.plate)
        }
    }

    private func queue() async {
        guard genericRunCapability.canRun else {
            errorMessage = genericRunCapability.explanation
            return
        }
        isRunning = true
        defer { isRunning = false }
        do {
            let inputs = try inputValues.reduce(
                into: [String: JSONValue]()
            ) { values, entry in
                let raw = entry.value.trimmingCharacters(in: .whitespacesAndNewlines)
                guard !raw.isEmpty else { return }
                values[entry.key] = try JSONDecoder().decode(
                    JSONValue.self,
                    from: Data(raw.utf8)
                )
            }
            _ = try await session.client.runPipeline(
                revision.reference.pipeline,
                version: revision.reference.version,
                flow: selectedFlow,
                inputs: inputs,
                parameters: parameterForm.values,
                priority: Int(priority),
                dryRun: mode.isDryRun
            )
            await session.refreshAll()
            dismiss()
        } catch {
            errorMessage = error.localizedDescription
        }
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
    @State private var viewport = PipelineCanvasViewport()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView([.horizontal, .vertical]) {
            ZStack {
                Canvas { context, size in
                    drawGrid(context: &context, size: size)
                    drawEdges(context: &context)
                }

                ForEach(model.groups) { group in
                    ZStack(alignment: .topLeading) {
                        RoundedRectangle(cornerRadius: 12)
                            .fill(theme.palette.cyan.opacity(0.04))
                            .overlay {
                                RoundedRectangle(cornerRadius: 12)
                                    .stroke(
                                        theme.palette.cyan.opacity(0.45),
                                        style: StrokeStyle(
                                            lineWidth: 1,
                                            dash: [7, 5]
                                        )
                                    )
                            }
                        Text(group.title)
                            .windexStyle(Typography.eyebrow)
                            .foregroundStyle(theme.palette.cyan)
                            .padding(.xs)
                    }
                    .frame(width: group.width, height: group.height)
                    .position(
                        x: group.x + group.width / 2,
                        y: group.y + group.height / 2
                    )
                }

                ForEach(model.annotations) { annotation in
                    Text(annotation.text)
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                        .padding(.sm)
                        .frame(
                            width: annotation.width,
                            height: annotation.height,
                            alignment: .topLeading
                        )
                        .background(theme.palette.plate.opacity(0.92))
                        .overlay(Rectangle().stroke(theme.palette.rule))
                        .position(
                            x: annotation.x + annotation.width / 2,
                            y: annotation.y + annotation.height / 2
                        )
                }

                if let flow = model.currentFlow {
                    ForEach(Array(flow.inputs.enumerated()), id: \.element.id) { index, input in
                        PipelineCanvasBoundary(
                            boundary: input,
                            owner: .input,
                            position: boundaryPosition(owner: .input, index: index),
                            connecting: model.pendingConnectionSource == .input(input.name),
                            compatible: nil,
                            semanticEditable: model.semanticEditable,
                            action: {
                                model.beginConnection(from: .input(input.name))
                            }
                        )
                    }

                    ForEach(Array(flow.nodes.enumerated()), id: \.element.id) { index, node in
                        let position = model.position(for: node.id, index: index)
                        let compatibility = registry.map {
                            model.pendingConnectionSource == nil ? nil
                                : model.canConnect(to: .node(node.id), registry: $0)
                        } ?? nil
                        PipelineCanvasNode(
                            node: node,
                            kind: registry?.kind(node.kind),
                            module: registry?.module(node.module),
                            position: position,
                            selected: model.selectedNodeID == node.id,
                            connecting: model.pendingConnectionSource == .node(node.id),
                            targetCompatibility: compatibility,
                            semanticEditable: model.semanticEditable,
                            translate: { origin, translation in
                                viewport.translatedPosition(
                                    origin: origin,
                                    translation: translation
                                )
                            },
                            select: {
                                model.select(node.id, registry: registry)
                                model.rightPane = .inspector
                            },
                            beginConnection: {
                                model.beginConnection(from: .node(node.id))
                            },
                            finishConnection: {
                                guard let registry else { return }
                                model.finishConnection(
                                    to: .node(node.id),
                                    registry: registry
                                )
                            },
                            move: { model.move(node.id, to: $0) })
                    }

                    ForEach(Array(flow.outputs.enumerated()), id: \.element.id) { index, output in
                        let compatibility = registry.map {
                            model.pendingConnectionSource == nil ? nil
                                : model.canConnect(to: .output(output.name), registry: $0)
                        } ?? nil
                        PipelineCanvasBoundary(
                            boundary: output,
                            owner: .output,
                            position: boundaryPosition(owner: .output, index: index),
                            connecting: false,
                            compatible: compatibility,
                            semanticEditable: model.semanticEditable,
                            action: {
                                guard let registry else { return }
                                model.finishConnection(
                                    to: .output(output.name),
                                    registry: registry
                                )
                            }
                        )
                    }
                }
            }
            .frame(width: 1600, height: 1000)
            .scaleEffect(viewport.zoom, anchor: .topLeading)
            .frame(
                width: 1600 * viewport.zoom,
                height: 1000 * viewport.zoom,
                alignment: .topLeading
            )
        }
        .background(theme.palette.ink)
        .overlay(alignment: .bottomTrailing) {
            HStack(spacing: .xs) {
                Button {
                    viewport.zoomOut()
                } label: {
                    Image(systemName: "minus")
                        .accessibilityLabel("Zoom out")
                }
                Slider(
                    value: Binding(
                        get: { viewport.zoom },
                        set: { viewport.setZoom($0) }
                    ),
                    in: 0.5...2,
                    step: 0.05
                )
                    .frame(width: 110)
                    .accessibilityLabel("Canvas zoom")
                Text("\(Int(viewport.zoom * 100))%")
                    .windexStyle(Typography.dataSM)
                    .frame(width: 40, alignment: .trailing)
                Button {
                    viewport.zoomIn()
                } label: {
                    Image(systemName: "plus")
                        .accessibilityLabel("Zoom in")
                }
            }
            .buttonStyle(.plain)
            .padding(.sm)
            .background(theme.palette.plate.opacity(0.95))
            .overlay(Rectangle().stroke(theme.palette.rule))
            .padding(.md)
        }
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

        for edge in flow.edges {
            guard let start = endpoint(
                edge.from,
                flow: flow,
                nodeIndexes: indexes,
                isSource: true
            ), let end = endpoint(
                edge.to,
                flow: flow,
                nodeIndexes: indexes,
                isSource: false
            ) else { continue }
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

    private func endpoint(
        _ reference: PipelinePortReference,
        flow: PipelineFlow,
        nodeIndexes: [String: Int],
        isSource: Bool
    ) -> CGPoint? {
        switch reference.owner {
        case .input:
            guard let index = flow.inputs.firstIndex(where: {
                $0.name == reference.id
            }) else { return nil }
            let center = boundaryPosition(owner: .input, index: index)
            return CGPoint(x: center.x + 66, y: center.y)
        case .node:
            guard let index = nodeIndexes[reference.id] else { return nil }
            let center = model.position(for: reference.id, index: index)
            return CGPoint(
                x: center.x + (isSource ? 106 : -106),
                y: center.y
            )
        case .output:
            guard let index = flow.outputs.firstIndex(where: {
                $0.name == reference.id
            }) else { return nil }
            let center = boundaryPosition(owner: .output, index: index)
            return CGPoint(x: center.x - 66, y: center.y)
        }
    }

    private func boundaryPosition(
        owner: PipelinePortReference.Owner,
        index: Int
    ) -> CGPoint {
        CGPoint(
            x: owner == .input ? 80 : 1520,
            y: 110 + CGFloat(index) * 86
        )
    }
}

private struct PipelineCanvasBoundary: View {
    let boundary: PipelineBoundary
    let owner: PipelinePortReference.Owner
    let position: CGPoint
    let connecting: Bool
    let compatible: Bool?
    let semanticEditable: Bool
    let action: () -> Void
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ZStack {
            VStack(alignment: owner == .input ? .leading : .trailing, spacing: .xxs) {
                Text(boundary.displayTitle)
                    .windexStyle(Typography.label)
                    .lineLimit(1)
                Text(boundary.type)
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
            .padding(.sm)
            .frame(width: 120, height: 58, alignment: owner == .input ? .leading : .trailing)
            .background(theme.palette.plate)
            .overlay {
                RoundedRectangle(cornerRadius: 8)
                    .stroke(strokeColour, lineWidth: connecting || compatible == true ? 2 : 1)
            }

            Button(action: action) {
                Circle()
                    .fill(connecting ? theme.palette.cyan : theme.palette.ink)
                    .overlay(Circle().stroke(strokeColour, lineWidth: 2))
                    .frame(width: 14, height: 14)
            }
            .buttonStyle(.plain)
            .disabled(
                !semanticEditable
                    || (owner == .output && compatible != true)
            )
            .accessibilityLabel(
                owner == .input
                    ? "Connect from Flow input \(boundary.name)"
                    : "Connect to Flow output \(boundary.name)"
            )
            .offset(x: owner == .input ? 60 : -60)
        }
        .opacity(compatible == false ? 0.45 : 1)
        .position(position)
    }

    private var strokeColour: Color {
        if compatible == true { return theme.palette.cyan }
        if compatible == false { return theme.palette.graphite }
        return connecting ? theme.palette.cyan : theme.palette.rule
    }
}

private struct PipelineCanvasNode: View {
    let node: PipelineNode
    let kind: PipelineKindDescriptor?
    let module: PipelineModuleDescriptor?
    let position: CGPoint
    let selected: Bool
    let connecting: Bool
    let targetCompatibility: Bool?
    let semanticEditable: Bool
    let translate: (CGPoint, CGSize) -> CGPoint
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
                        move(translate(origin, value.translation))
                    }
                    .onEnded { _ in dragOrigin = nil })

            if semanticEditable, kind?.inputType != nil {
                Button(action: finishConnection) {
                    Circle()
                        .fill(theme.palette.ink)
                        .overlay(Circle().stroke(theme.palette.cyan, lineWidth: 2))
                        .frame(width: 14, height: 14)
                }
                .buttonStyle(.plain)
                .disabled(targetCompatibility != true)
                .accessibilityLabel("Connect to \(node.id)")
                .offset(x: -100)
            }

            if semanticEditable, kind?.outputType != nil {
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
        .opacity(targetCompatibility == false ? 0.55 : 1)
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
                                .disabled(model.draft == nil || !model.semanticEditable)
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
    @Environment(BackendSession.self) private var session
    @State private var nodeName = ""
    @FocusState private var focusedBindingField: String?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                if let node = selectedNode {
                    VStack(alignment: .leading, spacing: .xs) {
                        StyledText(node.id, Typography.masthead)
                        TextField("Node name", text: $nodeName)
                            .textFieldStyle(.roundedBorder)
                            .disabled(!model.semanticEditable)
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
                                    .disabled(!model.semanticEditable)
                                }
                            }
                        }
                    }

                    if let module = registry?.module(node.module),
                       !module.fields.isEmpty {
                        Hairline()
                        StyledText("Field bindings", Typography.eyebrow)
                            .foregroundStyle(theme.palette.graphite)
                        ForEach(module.fields) { field in
                            bindingControl(field, node: node)
                        }
                    }

                    if let form = model.nodeForm, !form.params.isEmpty {
                        Hairline()
                        SchemaForm(model: form)
                            .disabled(!model.semanticEditable)
                        HStack {
                            Button("Apply configuration") {
                                guard let registry else { return }
                                model.applyNodeForm(registry: registry)
                            }
                            .disabled(
                                !model.semanticEditable
                                    || !form.isDirty
                                    || !form.errors.isEmpty
                            )
                            Spacer()
                        }
                    } else {
                        Text("This Module has no configuration fields.")
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.graphite)
                    }

                    Hairline()
                    HStack {
                        Button("Group Node") {
                            model.groupSelectedNode()
                            model.rightPane = .flow
                        }
                        Button("Duplicate Node") {
                            guard let registry else { return }
                            model.duplicateSelectedNode(registry: registry)
                        }
                        .keyboardShortcut("d", modifiers: .command)
                        .disabled(!model.semanticEditable)

                        Button("Remove Node", role: .destructive) {
                            guard let registry else { return }
                            model.removeSelectedNode(registry: registry)
                        }
                        .keyboardShortcut(.delete, modifiers: [])
                        .disabled(!model.semanticEditable)
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
                        Button {
                            focus(issue)
                        } label: {
                            HStack(alignment: .top, spacing: .xs) {
                                StatusBadge(
                                    issue.severity == .error ? .fault : .attention,
                                    word: issue.severity.rawValue)
                                VStack(alignment: .leading, spacing: .xxs) {
                                    Text(issue.message)
                                        .windexStyle(Typography.body)
                                    Text(issue.path)
                                        .windexStyle(Typography.dataSM)
                                        .foregroundStyle(theme.palette.graphite)
                                }
                            }
                        }
                        .buttonStyle(.plain)
                    }
                }

                if let message = model.serverValidationMessage {
                    Text("Server validation unavailable: \(message)")
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.amber)
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
        .task(id: model.focusedFieldKey) {
            focusedBindingField = model.focusedFieldKey
        }
    }

    private var selectedNode: PipelineNode? {
        guard let selectedNodeID = model.selectedNodeID else { return nil }
        return model.currentFlow?.nodes.first { $0.id == selectedNodeID }
    }

    @ViewBuilder
    private func bindingControl(_ field: Param, node: PipelineNode) -> some View {
        let options = NodeBindingOptions(
            field: field,
            parameters: model.draft?.parameters ?? [],
            configuredSecrets: session.sources.configuredSecrets
        )
        let value = node.config[field.key] ?? .literal(field.initialValue ?? .string(""))
        VStack(alignment: .leading, spacing: .xs) {
            Text(field.title)
                .windexStyle(Typography.label)
            Picker(
                "Binding",
                selection: Binding(
                    get: { value.bindingMode },
                    set: { mode in
                        guard let registry else { return }
                        let replacement: NodeConfigValue
                        switch mode {
                        case .literal:
                            replacement = .literal(field.initialValue ?? .string(""))
                        case .pipelineParameter:
                            guard let key = options.parameterKeys.first else { return }
                            replacement = .parameter(key)
                        case .secretReference:
                            guard let name = options.secretNames.first else { return }
                            replacement = .secret(name)
                        }
                        model.setNodeBinding(
                            field: field.key,
                            value: replacement,
                            registry: registry
                        )
                    }
                )
            ) {
                Text("Literal").tag(NodeBindingMode.literal)
                if options.supports(.pipelineParameter) {
                    Text("Pipeline parameter").tag(NodeBindingMode.pipelineParameter)
                }
                if options.supports(.secretReference) {
                    Text("Secret reference").tag(NodeBindingMode.secretReference)
                }
            }
            .pickerStyle(.segmented)
            .focused($focusedBindingField, equals: field.key)
            .disabled(!model.semanticEditable)

            switch value {
            case .parameter(let selected):
                Picker(
                    "Parameter",
                    selection: Binding(
                        get: { selected },
                        set: { key in
                            guard let registry else { return }
                            model.setNodeBinding(
                                field: field.key,
                                value: .parameter(key),
                                registry: registry
                            )
                        }
                    )
                ) {
                    ForEach(options.parameterKeys, id: \.self) { Text($0).tag($0) }
                }
                .disabled(!model.semanticEditable)
            case .secret(let selected):
                Picker(
                    "Secret",
                    selection: Binding(
                        get: { selected },
                        set: { name in
                            guard let registry else { return }
                            model.setNodeBinding(
                                field: field.key,
                                value: .secret(name),
                                registry: registry
                            )
                        }
                    )
                ) {
                    ForEach(options.secretNames, id: \.self) { Text($0).tag($0) }
                }
                .disabled(!model.semanticEditable)
            case .literal:
                Text("Edit the literal value in the form below.")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
        }
        .padding(.sm)
        .background(theme.palette.ink)
    }

    private func focus(_ issue: PipelineValidationIssue) {
        let previousFlow = model.selectedFlow
        guard let flow = model.focus(issue, registry: registry),
              flow != previousFlow,
              let draft = model.draft,
              let version = model.baseVersion else { return }
        model.apply(session.pipelines.layout(
            pipeline: draft.name,
            version: version,
            flow: flow
        ))
    }
}

private struct PipelineFlowInspector: View {
    @Bindable var model: PipelineComposerModel
    let registry: PipelineRegistry?
    @State private var flowName = ""
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
                        HStack {
                            TextField("Flow name", text: $flowName)
                                .textFieldStyle(.roundedBorder)
                                .disabled(!model.semanticEditable)
                            Button("Rename") {
                                model.renameSelectedFlow(
                                    to: flowName,
                                    registry: registry
                                )
                                flowName = model.selectedFlow
                            }
                            .disabled(
                                !model.semanticEditable
                                    || flowName.trimmingCharacters(
                                        in: .whitespacesAndNewlines
                                    ).isEmpty
                                    || flowName == flow.name
                            )
                        }
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
                    .disabled(!model.semanticEditable)

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
                    layoutObjects(flow: flow)

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
                    if !model.semanticEditable {
                        checklist(
                            "Published Source capability",
                            satisfied: model.sourceCapability.capable
                        )
                        ForEach(model.sourceCapability.issues) { issue in
                            Text("\(issue.path): \(issue.message)")
                                .windexStyle(Typography.dataSM)
                                .foregroundStyle(
                                    issue.severity == .error
                                        ? theme.palette.rust
                                        : theme.palette.amber
                                )
                        }
                    }
                } else {
                    Text("Choose a Flow.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                }
            }
            .padding(.md)
        }
        .task(id: model.selectedFlow) {
            flowName = model.selectedFlow
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
                .disabled(!model.semanticEditable)
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
                    .disabled(!model.semanticEditable)
                }
            }
        }
    }

    private func layoutObjects(flow: PipelineFlow) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            HStack {
                StyledText("Layout groups", Typography.eyebrow)
                    .foregroundStyle(theme.palette.graphite)
                Spacer()
                Button("Add annotation") {
                    model.addAnnotation()
                }
            }
            if model.groups.isEmpty {
                Text("Select a Node and choose Group Node to create a visual group.")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
            ForEach(model.groups) { group in
                VStack(alignment: .leading, spacing: .xs) {
                    HStack {
                        TextField(
                            "Group title",
                            text: Binding(
                                get: { group.title },
                                set: { title in
                                    var updated = group
                                    updated.title = title
                                    model.updateGroup(updated)
                                }
                            )
                        )
                        Menu("Nodes") {
                            ForEach(flow.nodes) { node in
                                Button {
                                    var updated = group
                                    if updated.nodes.contains(node.id) {
                                        updated.nodes.removeAll { $0 == node.id }
                                    } else {
                                        updated.nodes.append(node.id)
                                    }
                                    model.updateGroup(updated)
                                } label: {
                                    Label(
                                        node.id,
                                        systemImage: group.nodes.contains(node.id)
                                            ? "checkmark"
                                            : "circle"
                                    )
                                }
                            }
                        }
                        Button(role: .destructive) {
                            model.removeGroup(group.id)
                        } label: {
                            Image(systemName: "trash")
                        }
                        .buttonStyle(.plain)
                    }
                    Text(group.nodes.isEmpty
                         ? "No Nodes"
                         : group.nodes.joined(separator: ", "))
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                }
                .padding(.sm)
                .background(theme.palette.ink)
            }

            if !model.annotations.isEmpty {
                StyledText("Annotations", Typography.eyebrow)
                    .foregroundStyle(theme.palette.graphite)
            }
            ForEach(model.annotations) { annotation in
                HStack {
                    TextField(
                        "Annotation",
                        text: Binding(
                            get: { annotation.text },
                            set: { text in
                                var updated = annotation
                                updated.text = text
                                model.updateAnnotation(updated)
                            }
                        ),
                        axis: .vertical
                    )
                    Button(role: .destructive) {
                        model.removeAnnotation(annotation.id)
                    } label: {
                        Image(systemName: "trash")
                    }
                    .buttonStyle(.plain)
                }
                .padding(.sm)
                .background(theme.palette.ink)
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

private struct PipelineParameterInspector: View {
    @Bindable var model: PipelineComposerModel
    let registry: PipelineRegistry?
    @Environment(\.windexTheme) private var theme

    @State private var previousKey: String?
    @State private var key = ""
    @State private var title = ""
    @State private var detail = ""
    @State private var kind = "str"
    @State private var required = false
    @State private var stage = Param.Stage.runtime
    @State private var defaultJSON = ""
    @State private var choices = ""
    @State private var minimum = ""
    @State private var maximum = ""
    @State private var allowedSecrets = ""
    @State private var errorMessage: String?

    private let kinds: [(String, String)] = [
        ("String", "str"),
        ("Integer", "int"),
        ("Float", "float"),
        ("Boolean", "bool"),
        ("Choice", "choice"),
        ("URL", "url"),
        ("URL list", "url_list"),
        ("Date", "date"),
        ("Duration", "duration"),
        ("Secret reference", "secret_ref"),
    ]

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .lg) {
                HStack {
                    StyledText("Parameters", Typography.masthead)
                    Spacer()
                    Button("New") { beginNew() }
                        .disabled(!model.semanticEditable)
                }
                Text(
                    "Declare typed Pipeline values here. Node fields can bind to compatible declarations in the Node inspector."
                )
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)

                if let parameters = model.draft?.parameters, !parameters.isEmpty {
                    ForEach(parameters) { parameter in
                        Button {
                            edit(parameter)
                        } label: {
                            HStack {
                                VStack(alignment: .leading, spacing: .xxs) {
                                    Text(parameter.title)
                                        .windexStyle(Typography.label)
                                    Text(
                                        "\(parameter.key) · \(parameter.kind.rawValue) · \(parameter.stage.rawValue)"
                                    )
                                    .windexStyle(Typography.dataSM)
                                    .foregroundStyle(theme.palette.graphite)
                                }
                                Spacer()
                                if parameter.required {
                                    StatusBadge(.attention, word: "required")
                                }
                            }
                            .contentShape(Rectangle())
                        }
                        .buttonStyle(.plain)
                    }
                    Hairline()
                }

                if previousKey != nil || !key.isEmpty {
                    Group {
                        TextField("Key", text: $key)
                        TextField("Title", text: $title)
                        TextField("Description", text: $detail, axis: .vertical)
                    }
                    .textFieldStyle(.roundedBorder)
                    .disabled(!model.semanticEditable)

                    Picker("Kind", selection: $kind) {
                        ForEach(kinds, id: \.1) { value in
                            Text(value.0).tag(value.1)
                        }
                    }
                    .disabled(!model.semanticEditable)
                    Picker("Stage", selection: $stage) {
                        Text("Runtime").tag(Param.Stage.runtime)
                        Text("Install").tag(Param.Stage.install)
                    }
                    .disabled(!model.semanticEditable)
                    Toggle("Required", isOn: $required)
                        .disabled(!model.semanticEditable)
                    TextField(
                        "Default as JSON (optional)",
                        text: $defaultJSON
                    )
                    .textFieldStyle(.roundedBorder)
                    .disabled(!model.semanticEditable)
                    if kind == "choice" {
                        TextField("Choices, comma separated", text: $choices)
                            .textFieldStyle(.roundedBorder)
                            .disabled(!model.semanticEditable)
                    }
                    if kind == "int" || kind == "float" {
                        HStack {
                            TextField("Minimum (optional)", text: $minimum)
                            TextField("Maximum (optional)", text: $maximum)
                        }
                        .textFieldStyle(.roundedBorder)
                        .disabled(!model.semanticEditable)
                    }
                    if kind == "secret_ref" {
                        TextField(
                            "Allowed secret names, comma separated",
                            text: $allowedSecrets
                        )
                        .textFieldStyle(.roundedBorder)
                        .disabled(!model.semanticEditable)
                    }

                    HStack {
                        Button(previousKey == nil ? "Add parameter" : "Save parameter") {
                            save()
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(
                            !model.semanticEditable
                                || key.trimmingCharacters(in: .whitespaces).isEmpty
                        )
                        if let previousKey {
                            Button("Remove", role: .destructive) {
                                model.removeParameter(previousKey, registry: registry)
                                beginNew()
                            }
                            .disabled(!model.semanticEditable)
                        }
                    }
                } else {
                    Text("Choose a parameter or create a new declaration.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                }

                if let errorMessage {
                    Text(errorMessage)
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.rust)
                }
            }
            .padding(.md)
        }
        .task(id: model.focusedParameterKey) {
            guard let key = model.focusedParameterKey,
                  let parameter = model.draft?.parameters.first(where: {
                      $0.key == key
                  }) else { return }
            edit(parameter)
        }
    }

    private func beginNew() {
        previousKey = nil
        key = "parameter_\((model.draft?.parameters.count ?? 0) + 1)"
        title = ""
        detail = ""
        kind = "str"
        required = false
        stage = .runtime
        defaultJSON = ""
        choices = ""
        minimum = ""
        maximum = ""
        allowedSecrets = ""
        errorMessage = nil
    }

    private func edit(_ parameter: Param) {
        previousKey = parameter.key
        key = parameter.key
        title = parameter.title
        detail = parameter.description
        kind = parameter.kind.rawValue
        required = parameter.required
        stage = parameter.stage
        choices = parameter.choices.joined(separator: ", ")
        minimum = parameter.lo.map { String($0) } ?? ""
        maximum = parameter.hi.map { String($0) } ?? ""
        allowedSecrets = parameter.allow.joined(separator: ", ")
        if let value = parameter.defaultValue,
           let data = try? JSONEncoder().encode(value) {
            defaultJSON = String(decoding: data, as: UTF8.self)
        } else {
            defaultJSON = ""
        }
        errorMessage = nil
    }

    private func save() {
        do {
            let value: JSONValue?
            if defaultJSON.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                value = nil
            } else {
                value = try JSONDecoder().decode(
                    JSONValue.self,
                    from: Data(defaultJSON.utf8)
                )
            }
            let definition = PipelineParameterDefinition(
                key: key.trimmingCharacters(in: .whitespacesAndNewlines),
                kind: Param.Kind(rawValue: kind) ?? .unknown(kind),
                title: title,
                description: detail,
                required: required,
                stage: stage,
                defaultValue: value,
                choices: choices.split(separator: ",").map {
                    $0.trimmingCharacters(in: .whitespacesAndNewlines)
                }.filter { !$0.isEmpty },
                minimum: Double(minimum),
                maximum: Double(maximum),
                allowedSecrets: allowedSecrets.split(separator: ",").map {
                    $0.trimmingCharacters(in: .whitespacesAndNewlines)
                }.filter { !$0.isEmpty }
            )
            if let previousKey {
                model.updateParameter(
                    previousKey: previousKey,
                    definition: definition,
                    registry: registry
                )
            } else {
                model.addParameter(definition, registry: registry)
            }
            edit(try definition.parameter())
        } catch {
            errorMessage = error.localizedDescription
        }
    }
}
