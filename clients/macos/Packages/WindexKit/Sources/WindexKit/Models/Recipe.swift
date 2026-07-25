import Foundation

/// The compact graph description returned alongside a recipe.
///
/// The server intentionally leaves `Recipe.flows` open in its response model:
/// flow names are dynamic keys. This is the stable value behind each key.
public struct RecipeFlowSummary: Codable, Hashable, Sendable {
    public let nodes: [String]
    public let edges: [[String]]

    public init(nodes: [String], edges: [[String]]) {
        self.nodes = nodes
        self.edges = edges
    }
}

/// One row from `GET /admin/v1/recipes/{name}/tasks`.
///
/// Task entries are deliberately open objects in the server response model,
/// because the compiler may add placement metadata without revving the whole
/// response. These are the fields the worker contract currently guarantees;
/// additive fields remain safely ignored by `Decodable`.
public struct RecipeTask: Codable, Hashable, Sendable {
    public let node: String
    public let kind: String
    public let module: String
    public let lane: String
    public let config: [String: JSONValue]
    public let dependsOn: [String]
    public let preconditions: [String]
    public let weight: Double
    public let maxAttempts: Int
    public let leaseSeconds: Int

    public init(
        node: String,
        kind: String,
        module: String,
        lane: String,
        config: [String: JSONValue],
        dependsOn: [String],
        preconditions: [String],
        weight: Double,
        maxAttempts: Int,
        leaseSeconds: Int
    ) {
        self.node = node
        self.kind = kind
        self.module = module
        self.lane = lane
        self.config = config
        self.dependsOn = dependsOn
        self.preconditions = preconditions
        self.weight = weight
        self.maxAttempts = maxAttempts
        self.leaseSeconds = leaseSeconds
    }

    enum CodingKeys: String, CodingKey {
        case node, kind, module, lane, config, preconditions, weight
        case dependsOn = "depends_on"
        case maxAttempts = "max_attempts"
        case leaseSeconds = "lease_seconds"
    }
}

extension Recipe {
    /// The title shown in lists, falling back to the stable recipe name.
    public var displayTitle: String {
        guard let title, !title.isEmpty else { return name }
        return title
    }

    /// The dynamic flow map as a concrete, UI-friendly value.
    public func flowSummaries() throws -> [String: RecipeFlowSummary] {
        try flows?.additionalProperties.decode([String: RecipeFlowSummary].self) ?? [:]
    }

    /// The normalized recipe document, when the endpoint included it.
    public func document() throws -> [String: JSONValue]? {
        try spec?.additionalProperties.decode([String: JSONValue].self)
    }
}

extension RecipeTasks {
    /// Decode the compiler's open task rows into their stable placement fields.
    public func placements() throws -> [RecipeTask] {
        try tasks?.map {
            try $0.additionalProperties.decode(RecipeTask.self)
        } ?? []
    }
}
