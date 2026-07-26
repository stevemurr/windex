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
    public let sourceCapability: PipelineSourceCapability

    public init(
        reference: PipelineRevisionReference,
        parentVersion: Int? = nil,
        spec: PipelineSpec,
        registryVersion: Int,
        author: String = "",
        note: String = "",
        sourceCapability: PipelineSourceCapability = .init(capable: false)
    ) {
        self.reference = reference
        self.parentVersion = parentVersion
        self.spec = spec
        self.registryVersion = registryVersion
        self.author = author
        self.note = note
        self.sourceCapability = sourceCapability
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

/// One presentation-only group in a Flow layout.
///
/// Layout group objects are deliberately open in the epoch-2 contract. Keeping
/// the complete object preserves fields added by another client or a newer
/// backend while these accessors expose the fields used by the macOS composer.
public struct PipelineLayoutGroup: Codable, Hashable, Identifiable, Sendable {
    public var fields: [String: JSONValue]

    public init(fields: [String: JSONValue]) {
        self.fields = fields
    }

    public init(
        id: String,
        title: String,
        nodes: [String],
        x: Double,
        y: Double,
        width: Double = 320,
        height: Double = 180
    ) {
        fields = [
            "id": .string(id),
            "title": .string(title),
            "nodes": .array(nodes.map(JSONValue.string)),
            "x": .double(x),
            "y": .double(y),
            "width": .double(width),
            "height": .double(height),
        ]
    }

    public var id: String {
        fields["id"]?.stringValue
            ?? fields["title"]?.stringValue
            ?? "group-\(fields.hashValue)"
    }
    public var title: String {
        get { fields["title"]?.stringValue ?? id }
        set { fields["title"] = .string(newValue) }
    }
    public var nodes: [String] {
        get { fields["nodes"]?.arrayValue?.compactMap(\.stringValue) ?? [] }
        set { fields["nodes"] = .array(newValue.map(JSONValue.string)) }
    }
    public var x: Double {
        get { fields["x"]?.doubleValue ?? 0 }
        set { fields["x"] = .double(newValue) }
    }
    public var y: Double {
        get { fields["y"]?.doubleValue ?? 0 }
        set { fields["y"] = .double(newValue) }
    }
    public var width: Double {
        get { fields["width"]?.doubleValue ?? 320 }
        set { fields["width"] = .double(newValue) }
    }
    public var height: Double {
        get { fields["height"]?.doubleValue ?? 180 }
        set { fields["height"] = .double(newValue) }
    }

    public init(from decoder: Decoder) throws {
        fields = try [String: JSONValue](from: decoder)
    }

    public func encode(to encoder: Encoder) throws {
        try fields.encode(to: encoder)
    }
}

/// One presentation-only annotation in a Flow layout. Unknown keys round-trip.
public struct PipelineLayoutAnnotation: Codable, Hashable, Identifiable, Sendable {
    public var fields: [String: JSONValue]

    public init(fields: [String: JSONValue]) {
        self.fields = fields
    }

    public init(
        id: String,
        text: String,
        x: Double,
        y: Double,
        width: Double = 240,
        height: Double = 88
    ) {
        fields = [
            "id": .string(id),
            "text": .string(text),
            "x": .double(x),
            "y": .double(y),
            "width": .double(width),
            "height": .double(height),
        ]
    }

    public var id: String {
        fields["id"]?.stringValue
            ?? fields["text"]?.stringValue
            ?? "annotation-\(fields.hashValue)"
    }
    public var text: String {
        get { fields["text"]?.stringValue ?? "" }
        set { fields["text"] = .string(newValue) }
    }
    public var x: Double {
        get { fields["x"]?.doubleValue ?? 0 }
        set { fields["x"] = .double(newValue) }
    }
    public var y: Double {
        get { fields["y"]?.doubleValue ?? 0 }
        set { fields["y"] = .double(newValue) }
    }
    public var width: Double {
        get { fields["width"]?.doubleValue ?? 240 }
        set { fields["width"] = .double(newValue) }
    }
    public var height: Double {
        get { fields["height"]?.doubleValue ?? 88 }
        set { fields["height"] = .double(newValue) }
    }

    public init(from decoder: Decoder) throws {
        fields = try [String: JSONValue](from: decoder)
    }

    public func encode(to encoder: Encoder) throws {
        try fields.encode(to: encoder)
    }
}

/// Presentation state has its own ETag and is excluded from semantic revision hashes.
public struct PipelineFlowLayout: Codable, Hashable, Sendable {
    public let pipeline: String
    public let version: Int
    public let flow: String
    public var positions: [String: PipelineNodePosition]
    public var groups: [PipelineLayoutGroup]
    public var annotations: [PipelineLayoutAnnotation]
    public var etag: String?
    public var updatedAt: String?

    public init(
        pipeline: String,
        version: Int,
        flow: String,
        positions: [String: PipelineNodePosition] = [:],
        groups: [PipelineLayoutGroup] = [],
        annotations: [PipelineLayoutAnnotation] = [],
        etag: String? = nil,
        updatedAt: String? = nil
    ) {
        self.pipeline = pipeline
        self.version = version
        self.flow = flow
        self.positions = positions
        self.groups = groups
        self.annotations = annotations
        self.etag = etag
        self.updatedAt = updatedAt
    }

    /// The exact open layout object accepted by the epoch-2 PUT endpoint.
    public func wirePayload() throws -> [String: JSONValue] {
        [
            "nodes": try roundTrip(positions, as: JSONValue.self),
            "groups": try roundTrip(groups, as: JSONValue.self),
            "annotations": try roundTrip(annotations, as: JSONValue.self),
        ]
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
