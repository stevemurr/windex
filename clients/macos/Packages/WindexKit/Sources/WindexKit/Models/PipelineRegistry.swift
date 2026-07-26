import Foundation

public struct PipelinePortTypeDescriptor: Codable, Hashable, Identifiable, Sendable {
    public var id: String { name }

    public let name: String
    public let title: String
    public let fields: [String]

    public init(name: String, title: String, fields: [String] = []) {
        self.name = name
        self.title = title
        self.fields = fields
    }
}

public struct PipelineKindDescriptor: Codable, Hashable, Identifiable, Sendable {
    public let id: String
    public let title: String
    public let description: String
    public let inputType: String?
    public let outputType: String?
    public let stateful: Bool

    public init(
        id: String,
        title: String,
        description: String,
        inputType: String?,
        outputType: String?,
        stateful: Bool
    ) {
        self.id = id
        self.title = title
        self.description = description
        self.inputType = inputType
        self.outputType = outputType
        self.stateful = stateful
    }

    private enum CodingKeys: String, CodingKey {
        case id, title, stateful
        case description = "help"
        case inputType = "in"
        case outputType = "out"
    }
}

public struct PipelineModuleDescriptor: Codable, Hashable, Identifiable, Sendable {
    public let id: String
    public let kind: String
    public let version: String
    public let digest: String?
    public let title: String
    public let summary: String
    public let stability: String
    public let capabilities: [String]
    public let contractRoles: [String]
    public let allowedHosts: [String]
    public let lane: String
    public let preconditions: [String]
    public let fields: [Param]
    public let implemented: Bool

    public init(
        id: String,
        kind: String,
        version: String,
        digest: String? = nil,
        title: String,
        summary: String = "",
        stability: String = "stable",
        capabilities: [String] = [],
        contractRoles: [String] = [],
        allowedHosts: [String] = [],
        lane: String = "io",
        preconditions: [String] = [],
        fields: [Param] = [],
        implemented: Bool = true
    ) {
        self.id = id
        self.kind = kind
        self.version = version
        self.digest = digest
        self.title = title
        self.summary = summary
        self.stability = stability
        self.capabilities = capabilities
        self.contractRoles = contractRoles
        self.allowedHosts = allowedHosts
        self.lane = lane
        self.preconditions = preconditions
        self.fields = fields
        self.implemented = implemented
    }

    private enum CodingKeys: String, CodingKey {
        case id, kind, version, digest, title, summary, stability, capabilities
        case contractRoles = "contract_roles"
        case allowedHosts = "allowed_hosts"
        case lane, preconditions, config, implemented
    }

    private struct Config: Codable, Hashable, Sendable {
        let fields: [Param]
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(String.self, forKey: .id)
        kind = try c.decode(String.self, forKey: .kind)
        version = try c.decodeIfPresent(String.self, forKey: .version) ?? "1"
        digest = try c.decodeIfPresent(String.self, forKey: .digest)
        title = try c.decodeIfPresent(String.self, forKey: .title) ?? id
        summary = try c.decodeIfPresent(String.self, forKey: .summary) ?? ""
        stability = try c.decodeIfPresent(String.self, forKey: .stability) ?? "stable"
        capabilities = try c.decodeIfPresent([String].self, forKey: .capabilities) ?? []
        contractRoles = try c.decodeIfPresent([String].self, forKey: .contractRoles) ?? []
        allowedHosts = try c.decodeIfPresent([String].self, forKey: .allowedHosts) ?? []
        lane = try c.decodeIfPresent(String.self, forKey: .lane) ?? "io"
        preconditions = try c.decodeIfPresent([String].self, forKey: .preconditions) ?? []
        fields = try c.decodeIfPresent(Config.self, forKey: .config)?.fields ?? []
        implemented = try c.decodeIfPresent(Bool.self, forKey: .implemented) ?? false
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(id, forKey: .id)
        try c.encode(kind, forKey: .kind)
        try c.encode(version, forKey: .version)
        try c.encodeIfPresent(digest, forKey: .digest)
        try c.encode(title, forKey: .title)
        try c.encode(summary, forKey: .summary)
        try c.encode(stability, forKey: .stability)
        try c.encode(capabilities, forKey: .capabilities)
        try c.encode(contractRoles, forKey: .contractRoles)
        try c.encode(allowedHosts, forKey: .allowedHosts)
        try c.encode(lane, forKey: .lane)
        try c.encode(preconditions, forKey: .preconditions)
        try c.encode(Config(fields: fields), forKey: .config)
        try c.encode(implemented, forKey: .implemented)
    }
}

public struct PipelineRegistry: Codable, Hashable, Sendable {
    public let version: Int
    public let portTypes: [PipelinePortTypeDescriptor]
    public let kinds: [PipelineKindDescriptor]
    public let modules: [PipelineModuleDescriptor]
    public let alwaysBeforeLoad: [String]

    public init(
        version: Int,
        portTypes: [PipelinePortTypeDescriptor],
        kinds: [PipelineKindDescriptor],
        modules: [PipelineModuleDescriptor],
        alwaysBeforeLoad: [String] = []
    ) {
        self.version = version
        self.portTypes = portTypes
        self.kinds = kinds
        self.modules = modules
        self.alwaysBeforeLoad = alwaysBeforeLoad
    }

    public func kind(_ id: String) -> PipelineKindDescriptor? {
        kinds.first { $0.id == id }
    }

    public func module(_ id: String) -> PipelineModuleDescriptor? {
        modules.first { $0.id == id }
    }
}

// This adapter is isolated to WindexKit while the backend agent replaces the
// open registry schema with typed generated DTOs. WindexApp only sees
// PipelineRegistry and needs no changes when the generated wire shape lands.
extension Registry {
    public func pipelineRegistry() throws -> PipelineRegistry {
        struct PortPayload: Codable {
            let title: String
            let fields: [String]
        }

        let decodedPorts = try portTypes?.additionalProperties
            .decode([String: PortPayload].self) ?? [:]
        let ports = decodedPorts
            .map {
                PipelinePortTypeDescriptor(
                    name: $0.key,
                    title: $0.value.title,
                    fields: $0.value.fields)
            }
            .sorted { $0.name < $1.name }

        let decodedKinds = try kinds?.map {
            try $0.additionalProperties.decode(PipelineKindDescriptor.self)
        } ?? []
        let decodedModules = try modules?.map {
            try $0.additionalProperties.decode(PipelineModuleDescriptor.self)
        } ?? []

        return PipelineRegistry(
            version: registryVersion,
            portTypes: ports,
            kinds: decodedKinds,
            modules: decodedModules,
            alwaysBeforeLoad: alwaysBeforeLoad ?? [])
    }
}
