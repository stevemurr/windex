import Foundation

public enum PipelineDraftError: LocalizedError, Equatable, Sendable {
    case duplicateFlow(String)
    case unknownFlow(String)
    case duplicateNode(String)
    case unknownNode(String)
    case duplicateEdge
    case invalidConnection(String)

    public var errorDescription: String? {
        switch self {
        case .duplicateFlow(let name):
            "A Flow named “\(name)” already exists."
        case .unknownFlow(let name):
            "The Flow “\(name)” does not exist."
        case .duplicateNode(let id):
            "A Node named “\(id)” already exists in this Flow."
        case .unknownNode(let id):
            "The Node “\(id)” does not exist in this Flow."
        case .duplicateEdge:
            "That connection already exists."
        case .invalidConnection(let message):
            message
        }
    }
}

public struct PipelineDraft: Codable, Hashable, Sendable {
    public var name: String
    public var title: String
    public var description: String
    public var parameters: [Param]
    public var flows: [PipelineFlow]
    public var refreshFlows: [String]

    public init(
        name: String,
        title: String,
        description: String = "",
        parameters: [Param] = [],
        flows: [PipelineFlow] = [PipelineFlow(name: "main")],
        refreshFlows: [String] = []
    ) {
        self.name = name
        self.title = title
        self.description = description
        self.parameters = parameters
        self.flows = flows
        self.refreshFlows = refreshFlows
    }

    public init(revision: PipelineRevision) {
        name = revision.reference.pipeline
        title = revision.spec.title
        description = revision.spec.description
        parameters = revision.spec.parameters
        flows = revision.spec.flows
        refreshFlows = revision.spec.refreshFlows
    }

    public var spec: PipelineSpec {
        PipelineSpec(
            title: title,
            description: description,
            parameters: parameters,
            flows: flows,
            refreshFlows: refreshFlows)
    }

    public mutating func addFlow(named rawName: String) throws {
        let name = Self.normalizedIdentifier(rawName)
        guard !flows.contains(where: { $0.name == name }) else {
            throw PipelineDraftError.duplicateFlow(name)
        }
        flows.append(PipelineFlow(name: name))
    }

    public mutating func removeFlow(named name: String) throws {
        guard let index = flows.firstIndex(where: { $0.name == name }) else {
            throw PipelineDraftError.unknownFlow(name)
        }
        flows.remove(at: index)
        refreshFlows.removeAll { $0 == name }
    }

    @discardableResult
    public mutating func duplicateFlow(
        named sourceName: String,
        as rawName: String
    ) throws -> PipelineFlow {
        guard let source = flows.first(where: { $0.name == sourceName }) else {
            throw PipelineDraftError.unknownFlow(sourceName)
        }
        let name = Self.normalizedIdentifier(rawName)
        guard !flows.contains(where: { $0.name == name }) else {
            throw PipelineDraftError.duplicateFlow(name)
        }
        let copy = PipelineFlow(
            name: name,
            inputs: source.inputs,
            outputs: source.outputs,
            nodes: source.nodes,
            edges: source.edges)
        flows.append(copy)
        return copy
    }

    public mutating func renameFlow(named oldName: String, to rawName: String) throws {
        guard let index = flows.firstIndex(where: { $0.name == oldName }) else {
            throw PipelineDraftError.unknownFlow(oldName)
        }
        let name = Self.normalizedIdentifier(rawName)
        guard oldName == name || !flows.contains(where: { $0.name == name }) else {
            throw PipelineDraftError.duplicateFlow(name)
        }
        let old = flows[index]
        flows[index] = PipelineFlow(
            name: name,
            inputs: old.inputs,
            outputs: old.outputs,
            nodes: old.nodes,
            edges: old.edges)
        refreshFlows = refreshFlows.map { $0 == oldName ? name : $0 }
    }

    public mutating func setRefresh(_ enabled: Bool, forFlow name: String) throws {
        guard flows.contains(where: { $0.name == name }) else {
            throw PipelineDraftError.unknownFlow(name)
        }
        if enabled {
            if !refreshFlows.contains(name) { refreshFlows.append(name) }
        } else {
            refreshFlows.removeAll { $0 == name }
        }
    }

    public mutating func setBoundaries(
        inputs: [PipelineBoundary],
        outputs: [PipelineBoundary],
        forFlow name: String
    ) throws {
        guard let index = flows.firstIndex(where: { $0.name == name }) else {
            throw PipelineDraftError.unknownFlow(name)
        }
        let flow = flows[index]
        flows[index] = PipelineFlow(
            name: flow.name,
            inputs: inputs,
            outputs: outputs,
            nodes: flow.nodes,
            edges: flow.edges)
    }

    @discardableResult
    public mutating func addNode(
        module: PipelineModuleDescriptor,
        toFlow flowName: String
    ) throws -> PipelineNode {
        guard let flowIndex = flows.firstIndex(where: { $0.name == flowName }) else {
            throw PipelineDraftError.unknownFlow(flowName)
        }
        let existing = Set(flows[flowIndex].nodes.map(\.id))
        let base = Self.normalizedIdentifier(
            module.id.split(separator: ".").last.map(String.init) ?? module.id)
        var candidate = base
        var suffix = 2
        while existing.contains(candidate) {
            candidate = "\(base)_\(suffix)"
            suffix += 1
        }
        let config = Dictionary(
            uniqueKeysWithValues: module.fields.compactMap { field in
                field.initialValue.map { (field.key, NodeConfigValue.literal($0)) }
            })
        let node = PipelineNode(
            id: candidate,
            kind: module.kind,
            module: module.id,
            config: config)
        let flow = flows[flowIndex]
        flows[flowIndex] = PipelineFlow(
            name: flow.name,
            inputs: flow.inputs,
            outputs: flow.outputs,
            nodes: flow.nodes + [node],
            edges: flow.edges)
        return node
    }

    public mutating func removeNode(_ nodeID: String, fromFlow flowName: String) throws {
        guard let flowIndex = flows.firstIndex(where: { $0.name == flowName }) else {
            throw PipelineDraftError.unknownFlow(flowName)
        }
        let flow = flows[flowIndex]
        guard flow.nodes.contains(where: { $0.id == nodeID }) else {
            throw PipelineDraftError.unknownNode(nodeID)
        }
        flows[flowIndex] = PipelineFlow(
            name: flow.name,
            inputs: flow.inputs,
            outputs: flow.outputs,
            nodes: flow.nodes.filter { $0.id != nodeID },
            edges: flow.edges.filter { $0.from.id != nodeID && $0.to.id != nodeID })
    }

    @discardableResult
    public mutating func duplicateNode(
        _ nodeID: String,
        inFlow flowName: String
    ) throws -> PipelineNode {
        guard let flowIndex = flows.firstIndex(where: { $0.name == flowName }) else {
            throw PipelineDraftError.unknownFlow(flowName)
        }
        let flow = flows[flowIndex]
        guard let source = flow.nodes.first(where: { $0.id == nodeID }) else {
            throw PipelineDraftError.unknownNode(nodeID)
        }
        let existing = Set(flow.nodes.map(\.id))
        var suffix = 2
        var candidate = "\(source.id)_copy"
        while existing.contains(candidate) {
            candidate = "\(source.id)_copy_\(suffix)"
            suffix += 1
        }
        let copy = PipelineNode(
            id: candidate,
            kind: source.kind,
            module: source.module,
            config: source.config)
        flows[flowIndex] = PipelineFlow(
            name: flow.name,
            inputs: flow.inputs,
            outputs: flow.outputs,
            nodes: flow.nodes + [copy],
            edges: flow.edges)
        return copy
    }

    public mutating func renameNode(
        _ nodeID: String,
        to rawID: String,
        inFlow flowName: String
    ) throws {
        guard let flowIndex = flows.firstIndex(where: { $0.name == flowName }) else {
            throw PipelineDraftError.unknownFlow(flowName)
        }
        let flow = flows[flowIndex]
        guard let nodeIndex = flow.nodes.firstIndex(where: { $0.id == nodeID }) else {
            throw PipelineDraftError.unknownNode(nodeID)
        }
        let newID = Self.normalizedIdentifier(rawID)
        guard nodeID == newID || !flow.nodes.contains(where: { $0.id == newID }) else {
            throw PipelineDraftError.duplicateNode(newID)
        }
        var nodes = flow.nodes
        let node = nodes[nodeIndex]
        nodes[nodeIndex] = PipelineNode(
            id: newID,
            kind: node.kind,
            module: node.module,
            config: node.config)
        let edges = flow.edges.map { edge in
            PipelineEdge(
                from: Self.renaming(edge.from, nodeID, to: newID),
                to: Self.renaming(edge.to, nodeID, to: newID))
        }
        flows[flowIndex] = PipelineFlow(
            name: flow.name,
            inputs: flow.inputs,
            outputs: flow.outputs,
            nodes: nodes,
            edges: edges)
    }

    public mutating func updateNode(
        _ node: PipelineNode,
        inFlow flowName: String
    ) throws {
        guard let flowIndex = flows.firstIndex(where: { $0.name == flowName }) else {
            throw PipelineDraftError.unknownFlow(flowName)
        }
        let flow = flows[flowIndex]
        guard let nodeIndex = flow.nodes.firstIndex(where: { $0.id == node.id }) else {
            throw PipelineDraftError.unknownNode(node.id)
        }
        var nodes = flow.nodes
        nodes[nodeIndex] = node
        flows[flowIndex] = PipelineFlow(
            name: flow.name,
            inputs: flow.inputs,
            outputs: flow.outputs,
            nodes: nodes,
            edges: flow.edges)
    }

    public mutating func connect(
        _ edge: PipelineEdge,
        inFlow flowName: String,
        registry: PipelineRegistry
    ) throws {
        guard let flowIndex = flows.firstIndex(where: { $0.name == flowName }) else {
            throw PipelineDraftError.unknownFlow(flowName)
        }
        let flow = flows[flowIndex]
        guard !flow.edges.contains(edge) else {
            throw PipelineDraftError.duplicateEdge
        }
        let connectionIssues = PipelineLocalValidator.connectionIssues(
            edge: edge,
            flow: flow,
            registry: registry)
        if let issue = connectionIssues.first {
            throw PipelineDraftError.invalidConnection(issue.message)
        }
        let candidate = PipelineFlow(
            name: flow.name,
            inputs: flow.inputs,
            outputs: flow.outputs,
            nodes: flow.nodes,
            edges: flow.edges + [edge])
        if PipelineLocalValidator.hasCycle(candidate) {
            throw PipelineDraftError.invalidConnection(
                "That connection would create a cycle.")
        }
        flows[flowIndex] = candidate
    }

    public mutating func disconnect(
        _ edge: PipelineEdge,
        inFlow flowName: String
    ) throws {
        guard let flowIndex = flows.firstIndex(where: { $0.name == flowName }) else {
            throw PipelineDraftError.unknownFlow(flowName)
        }
        let flow = flows[flowIndex]
        flows[flowIndex] = PipelineFlow(
            name: flow.name,
            inputs: flow.inputs,
            outputs: flow.outputs,
            nodes: flow.nodes,
            edges: flow.edges.filter { $0 != edge })
    }

    private static func renaming(
        _ reference: PipelinePortReference,
        _ oldID: String,
        to newID: String
    ) -> PipelinePortReference {
        guard reference.owner == .node, reference.id == oldID else {
            return reference
        }
        return .node(newID)
    }

    private static func normalizedIdentifier(_ value: String) -> String {
        let lower = value.lowercased()
        let scalars = lower.unicodeScalars.map { scalar -> Character in
            CharacterSet.alphanumerics.contains(scalar) || scalar == "_"
                ? Character(String(scalar)) : "_"
        }
        var result = String(scalars)
        while result.contains("__") {
            result = result.replacingOccurrences(of: "__", with: "_")
        }
        result = result.trimmingCharacters(in: CharacterSet(charactersIn: "_"))
        if result.isEmpty { return "flow" }
        if result.first?.isNumber == true { result = "n_\(result)" }
        return result
    }
}

public enum PipelineLocalValidator {
    public static func validate(
        _ draft: PipelineDraft,
        registry: PipelineRegistry
    ) -> [PipelineValidationIssue] {
        var issues: [PipelineValidationIssue] = []
        let flowNames = draft.flows.map(\.name)
        for duplicate in duplicates(in: flowNames) {
            issues.append(issue(
                path: "flows.\(duplicate)",
                code: "duplicate_flow",
                "Flow names must be unique."))
        }
        for refresh in draft.refreshFlows where !flowNames.contains(refresh) {
            issues.append(issue(
                path: "refresh.\(refresh)",
                code: "unknown_refresh_flow",
                "Refresh names a Flow that does not exist."))
        }
        for flow in draft.flows {
            issues.append(contentsOf: validate(flow, registry: registry))
        }
        return issues
    }

    public static func validate(
        _ flow: PipelineFlow,
        registry: PipelineRegistry
    ) -> [PipelineValidationIssue] {
        var issues: [PipelineValidationIssue] = []
        for duplicate in duplicates(in: flow.nodes.map(\.id)) {
            issues.append(issue(
                path: "flows.\(flow.name).nodes.\(duplicate)",
                code: "duplicate_node",
                "Node IDs must be unique within a Flow."))
        }
        for node in flow.nodes {
            guard let module = registry.module(node.module) else {
                issues.append(issue(
                    path: "flows.\(flow.name).nodes.\(node.id).module",
                    code: "unknown_module",
                    "Module “\(node.module)” is not in this backend’s registry."))
                continue
            }
            if module.kind != node.kind {
                issues.append(issue(
                    path: "flows.\(flow.name).nodes.\(node.id).kind",
                    code: "module_kind_mismatch",
                    "Module “\(module.id)” is \(module.kind), not \(node.kind)."))
            }
            if !module.implemented {
                issues.append(PipelineValidationIssue(
                    path: "flows.\(flow.name).nodes.\(node.id).module",
                    code: "module_unavailable",
                    severity: .warning,
                    message: "Module “\(module.id)” is declared but unavailable."))
            }
        }
        for edge in flow.edges {
            issues.append(contentsOf: connectionIssues(
                edge: edge,
                flow: flow,
                registry: registry))
        }
        for duplicate in duplicates(in: flow.edges.map(\.id)) {
            issues.append(issue(
                path: "flows.\(flow.name).edges.\(duplicate)",
                code: "duplicate_edge",
                "Each connection may appear only once."))
        }
        if hasCycle(flow) {
            issues.append(issue(
                path: "flows.\(flow.name).edges",
                code: "cycle",
                "A Flow must be acyclic."))
        }
        return issues
    }

    public static func connectionIssues(
        edge: PipelineEdge,
        flow: PipelineFlow,
        registry: PipelineRegistry
    ) -> [PipelineValidationIssue] {
        let path = "flows.\(flow.name).edges.\(edge.id)"
        guard edge.from.owner != .output else {
            return [issue(path: path, code: "invalid_edge_source",
                          "A Flow output cannot start a connection.")]
        }
        guard edge.to.owner != .input else {
            return [issue(path: path, code: "invalid_edge_target",
                          "A Flow input cannot end a connection.")]
        }
        guard let output = outputType(
            for: edge.from, flow: flow, registry: registry) else {
            return [issue(path: path, code: "unknown_edge_source",
                          "The connection source does not exist or has no output.")]
        }
        guard let input = inputType(
            for: edge.to, flow: flow, registry: registry) else {
            return [issue(path: path, code: "unknown_edge_target",
                          "The connection target does not exist or has no input.")]
        }
        guard output == input else {
            return [issue(
                path: path,
                code: "port_type_mismatch",
                "Cannot connect \(output) to \(input).")]
        }
        return []
    }

    public static func hasCycle(_ flow: PipelineFlow) -> Bool {
        let nodeIDs = Set(flow.nodes.map(\.id))
        let edges = flow.edges.compactMap { edge -> (String, String)? in
            guard edge.from.owner == .node, edge.to.owner == .node,
                  nodeIDs.contains(edge.from.id), nodeIDs.contains(edge.to.id)
            else { return nil }
            return (edge.from.id, edge.to.id)
        }
        var incoming = Dictionary(uniqueKeysWithValues: nodeIDs.map { ($0, 0) })
        var outgoing: [String: [String]] = [:]
        for (from, to) in edges {
            incoming[to, default: 0] += 1
            outgoing[from, default: []].append(to)
        }
        var queue = incoming.filter { $0.value == 0 }.map(\.key)
        var visited = 0
        while let node = queue.popLast() {
            visited += 1
            for target in outgoing[node, default: []] {
                incoming[target, default: 0] -= 1
                if incoming[target] == 0 {
                    queue.append(target)
                }
            }
        }
        return visited != nodeIDs.count
    }

    private static func outputType(
        for reference: PipelinePortReference,
        flow: PipelineFlow,
        registry: PipelineRegistry
    ) -> String? {
        switch reference.owner {
        case .input:
            flow.inputs.first { $0.name == reference.id }?.type
        case .node:
            flow.nodes.first { $0.id == reference.id }
                .flatMap { registry.kind($0.kind)?.outputType }
        case .output:
            nil
        }
    }

    private static func inputType(
        for reference: PipelinePortReference,
        flow: PipelineFlow,
        registry: PipelineRegistry
    ) -> String? {
        switch reference.owner {
        case .input:
            nil
        case .node:
            flow.nodes.first { $0.id == reference.id }
                .flatMap { registry.kind($0.kind)?.inputType }
        case .output:
            flow.outputs.first { $0.name == reference.id }?.type
        }
    }

    private static func duplicates<T: Hashable>(in values: [T]) -> [T] {
        var seen = Set<T>()
        var duplicates = Set<T>()
        for value in values where !seen.insert(value).inserted {
            duplicates.insert(value)
        }
        return Array(duplicates)
    }

    private static func issue(
        path: String,
        code: String,
        _ message: String
    ) -> PipelineValidationIssue {
        PipelineValidationIssue(
            path: path,
            code: code,
            severity: .error,
            message: message)
    }
}
