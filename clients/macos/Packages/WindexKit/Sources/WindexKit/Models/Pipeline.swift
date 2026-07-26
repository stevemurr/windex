import Foundation

/// A stable pointer to one immutable Pipeline revision.
public struct PipelineRevisionReference: Codable, Hashable, Sendable {
    public let pipeline: String
    public let version: Int
    public let specHash: String

    public init(pipeline: String, version: Int, specHash: String) {
        self.pipeline = pipeline
        self.version = version
        self.specHash = specHash
    }
}

/// The lightweight Pipeline row used by catalogues.
public struct PipelineSummary: Codable, Hashable, Identifiable, Sendable {
    public var id: String { name }

    public let name: String
    public let title: String
    public let description: String
    public let headVersion: Int
    public let headHash: String
    public let builtin: Bool
    public let archived: Bool
    public let deploymentCount: Int

    public init(
        name: String,
        title: String,
        description: String = "",
        headVersion: Int,
        headHash: String,
        builtin: Bool = false,
        archived: Bool = false,
        deploymentCount: Int = 0
    ) {
        self.name = name
        self.title = title
        self.description = description
        self.headVersion = headVersion
        self.headHash = headHash
        self.builtin = builtin
        self.archived = archived
        self.deploymentCount = deploymentCount
    }

    public var displayTitle: String {
        title.isEmpty ? name : title
    }
}

/// A named, typed boundary on a Flow.
public struct PipelineBoundary: Codable, Hashable, Identifiable, Sendable {
    public var id: String { name }

    public let name: String
    public let title: String
    public let type: String
    public let required: Bool

    public init(name: String, title: String = "", type: String, required: Bool = true) {
        self.name = name
        self.title = title
        self.type = type
        self.required = required
    }

    public var displayTitle: String {
        title.isEmpty ? name : title
    }
}

/// One endpoint of a graph Edge.
public struct PipelinePortReference: Codable, Hashable, Sendable {
    public enum Owner: String, Codable, Hashable, Sendable {
        case input
        case node
        case output
    }

    public let owner: Owner
    public let id: String

    public init(owner: Owner, id: String) {
        self.owner = owner
        self.id = id
    }

    public static func input(_ id: String) -> Self {
        .init(owner: .input, id: id)
    }

    public static func node(_ id: String) -> Self {
        .init(owner: .node, id: id)
    }

    public static func output(_ id: String) -> Self {
        .init(owner: .output, id: id)
    }
}

public struct PipelineEdge: Codable, Hashable, Identifiable, Sendable {
    public var id: String {
        "\(from.owner.rawValue):\(from.id)->\(to.owner.rawValue):\(to.id)"
    }

    public let from: PipelinePortReference
    public let to: PipelinePortReference

    public init(from: PipelinePortReference, to: PipelinePortReference) {
        self.from = from
        self.to = to
    }
}

/// A configured instance of a registered Module.
public struct PipelineNode: Codable, Hashable, Identifiable, Sendable {
    public let id: String
    public let kind: String
    public let module: String
    public let config: [String: NodeConfigValue]

    public init(
        id: String,
        kind: String,
        module: String,
        config: [String: NodeConfigValue] = [:]
    ) {
        self.id = id
        self.kind = kind
        self.module = module
        self.config = config
    }
}

/// A Node field can be a literal or a reference resolved when a Run is frozen.
public enum NodeConfigValue: Hashable, Sendable, Codable {
    case literal(JSONValue)
    case parameter(String)
    case secret(String)

    public init(wireValue value: JSONValue) {
        if let string = value.stringValue, string.hasPrefix("@param.") {
            self = .parameter(String(string.dropFirst("@param.".count)))
        } else if let string = value.stringValue, string.hasPrefix("@secret.") {
            self = .secret(String(string.dropFirst("@secret.".count)))
        } else {
            self = .literal(value)
        }
    }

    public init(from decoder: Decoder) throws {
        let value = try JSONValue(from: decoder)
        self.init(wireValue: value)
    }

    public func encode(to encoder: Encoder) throws {
        try wireValue.encode(to: encoder)
    }

    public var wireValue: JSONValue {
        switch self {
        case .literal(let value):
            value
        case .parameter(let key):
            .string("@param.\(key)")
        case .secret(let name):
            .string("@secret.\(name)")
        }
    }
}

public struct PipelineFlow: Codable, Hashable, Identifiable, Sendable {
    public var id: String { name }

    public let name: String
    public let inputs: [PipelineBoundary]
    public let outputs: [PipelineBoundary]
    public let nodes: [PipelineNode]
    public let edges: [PipelineEdge]

    public init(
        name: String,
        inputs: [PipelineBoundary] = [],
        outputs: [PipelineBoundary] = [],
        nodes: [PipelineNode] = [],
        edges: [PipelineEdge] = []
    ) {
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.nodes = nodes
        self.edges = edges
    }
}

/// The reusable, corpus-independent semantics stored in one revision.
public struct PipelineSpec: Codable, Hashable, Sendable {
    public let schema: String
    public let title: String
    public let description: String
    public let parameters: [Param]
    public let flows: [PipelineFlow]
    public let refreshFlows: [String]

    public init(
        schema: String = "windex.pipeline/1",
        title: String,
        description: String = "",
        parameters: [Param] = [],
        flows: [PipelineFlow],
        refreshFlows: [String] = []
    ) {
        self.schema = schema
        self.title = title
        self.description = description
        self.parameters = parameters
        self.flows = flows
        self.refreshFlows = refreshFlows
    }

    private enum CodingKeys: String, CodingKey {
        case schema, parameters, state, flows, refresh
    }

    private struct BoundaryWire: Codable {
        let id: String
        let type: String
    }

    private struct NodeWire: Codable {
        let kind: String
        let uses: String
        let with: [String: NodeConfigValue]
    }

    private struct EndpointWire: Codable {
        let input: String?
        let node: String?
        let output: String?

        init(_ reference: PipelinePortReference) {
            input = reference.owner == .input ? reference.id : nil
            node = reference.owner == .node ? reference.id : nil
            output = reference.owner == .output ? reference.id : nil
        }

        var reference: PipelinePortReference {
            if let input { return .input(input) }
            if let output { return .output(output) }
            return .node(node ?? "")
        }
    }

    private struct EdgeWire: Codable {
        let from: EndpointWire
        let to: EndpointWire
    }

    private struct FlowWire: Codable {
        let inputs: [BoundaryWire]
        let outputs: [BoundaryWire]
        let nodes: [String: NodeWire]
        let edges: [EdgeWire]
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        schema = try container.decode(String.self, forKey: .schema)
        title = ""
        description = ""
        parameters = try container.decodeIfPresent([Param].self, forKey: .parameters) ?? []
        refreshFlows = try container.decodeIfPresent([String].self, forKey: .refresh) ?? []
        let wireFlows = try container.decode([String: FlowWire].self, forKey: .flows)
        flows = wireFlows.map { name, flow in
            PipelineFlow(
                name: name,
                inputs: flow.inputs.map {
                    PipelineBoundary(name: $0.id, type: $0.type)
                },
                outputs: flow.outputs.map {
                    PipelineBoundary(name: $0.id, type: $0.type)
                },
                nodes: flow.nodes.map { id, node in
                    PipelineNode(id: id, kind: node.kind, module: node.uses,
                                 config: node.with)
                }.sorted { $0.id < $1.id },
                edges: flow.edges.map {
                    PipelineEdge(from: $0.from.reference, to: $0.to.reference)
                }
            )
        }.sorted { $0.name < $1.name }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(schema, forKey: .schema)
        try container.encode(parameters, forKey: .parameters)
        try container.encode([String: JSONValue](), forKey: .state)
        let wireFlows = Dictionary(uniqueKeysWithValues: flows.map { flow in
            (
                flow.name,
                FlowWire(
                    inputs: flow.inputs.map { .init(id: $0.name, type: $0.type) },
                    outputs: flow.outputs.map { .init(id: $0.name, type: $0.type) },
                    nodes: Dictionary(uniqueKeysWithValues: flow.nodes.map {
                        ($0.id, NodeWire(kind: $0.kind, uses: $0.module, with: $0.config))
                    }),
                    edges: flow.edges.map {
                        EdgeWire(from: EndpointWire($0.from), to: EndpointWire($0.to))
                    }
                )
            )
        })
        try container.encode(wireFlows, forKey: .flows)
        try container.encode(refreshFlows, forKey: .refresh)
    }
}

public struct PipelineRevision: Codable, Hashable, Identifiable, Sendable {
    public var id: PipelineRevisionReference { reference }

    public let reference: PipelineRevisionReference
    public let parentVersion: Int?
    public let spec: PipelineSpec
    public let registryVersion: Int
    public let author: String
    public let note: String

    public init(
        reference: PipelineRevisionReference,
        parentVersion: Int? = nil,
        spec: PipelineSpec,
        registryVersion: Int,
        author: String = "",
        note: String = ""
    ) {
        self.reference = reference
        self.parentVersion = parentVersion
        self.spec = spec
        self.registryVersion = registryVersion
        self.author = author
        self.note = note
    }
}

public struct PipelineNodePosition: Codable, Hashable, Sendable {
    public var x: Double
    public var y: Double

    public init(x: Double, y: Double) {
        self.x = x
        self.y = y
    }
}

/// Presentation state has its own ETag and is excluded from semantic revision hashes.
public struct PipelineFlowLayout: Codable, Hashable, Sendable {
    public let pipeline: String
    public let version: Int
    public let flow: String
    public var positions: [String: PipelineNodePosition]
    public var groups: [String: [String]]
    public var annotations: [String: String]
    public var etag: String?

    public init(
        pipeline: String,
        version: Int,
        flow: String,
        positions: [String: PipelineNodePosition] = [:],
        groups: [String: [String]] = [:],
        annotations: [String: String] = [:],
        etag: String? = nil
    ) {
        self.pipeline = pipeline
        self.version = version
        self.flow = flow
        self.positions = positions
        self.groups = groups
        self.annotations = annotations
        self.etag = etag
    }
}

public struct PipelineValidationIssue: Codable, Hashable, Identifiable, Sendable {
    public enum Severity: String, Codable, Hashable, Sendable {
        case warning
        case error
    }

    public var id: String { "\(severity.rawValue):\(code):\(path)" }

    public let path: String
    public let code: String
    public let severity: Severity
    public let message: String

    public init(path: String, code: String, severity: Severity, message: String) {
        self.path = path
        self.code = code
        self.severity = severity
        self.message = message
    }
}

public struct PipelineSourceCapability: Codable, Hashable, Sendable {
    public let capable: Bool
    public let issues: [PipelineValidationIssue]

    public init(capable: Bool, issues: [PipelineValidationIssue] = []) {
        self.capable = capable
        self.issues = issues
    }
}
