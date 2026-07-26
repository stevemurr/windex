import Foundation

public struct SourceCreateRequest: Codable, Hashable, Sendable {
    public let name: String
    public let title: String
    public let description: String
    public let origin: [String: JSONValue]
    public let pipelineName: String
    public let pipelineVersion: Int?
    public let values: [String: JSONValue]
    public let searchName: String
    public let idPrefix: String
    public let collectionKey: String
    public let searchProfile: String
    public let includeInAll: Bool
    public let stateNamespace: String
    public let enabled: Bool
    enum CodingKeys: String, CodingKey {
        case name, title, description, origin, values, enabled
        case pipelineName = "pipeline_name"
        case pipelineVersion = "pipeline_version"
        case searchName = "search_name"
        case idPrefix = "id_prefix"
        case collectionKey = "collection_key"
        case searchProfile = "search_profile"
        case includeInAll = "include_in_all"
        case stateNamespace = "state_namespace"
    }

    public init(name: String, title: String = "", description: String = "",
                origin: [String: JSONValue] = [:], pipelineName: String,
                pipelineVersion: Int? = nil, values: [String: JSONValue] = [:],
                searchName: String, idPrefix: String, collectionKey: String,
                searchProfile: String, includeInAll: Bool = true,
                stateNamespace: String, enabled: Bool = true) {
        self.name = name; self.title = title; self.description = description
        self.origin = origin; self.pipelineName = pipelineName
        self.pipelineVersion = pipelineVersion; self.values = values
        self.searchName = searchName; self.idPrefix = idPrefix
        self.collectionKey = collectionKey; self.searchProfile = searchProfile
        self.includeInAll = includeInAll; self.stateNamespace = stateNamespace
        self.enabled = enabled
    }
}

private struct SourcePatchBody: Codable, Sendable {
    let title: String?
    let description: String?
    let origin: [String: JSONValue]?
    let enabled: Bool?
    let includeInAll: Bool?
    enum CodingKeys: String, CodingKey {
        case title, description, origin, enabled
        case includeInAll = "include_in_all"
    }
}
private struct SourceRunBody: Codable, Sendable {
    let flow: String?
    let inputs: [String: JSONValue]
    let overrides: [String: JSONValue]
    let priority: Int
}
private struct SourceSettingsPatchBody: Codable, Sendable {
    let values: [String: JSONValue]
}
private struct PauseBody: Codable, Sendable { let reason: String }
private struct ResetBody: Codable, Sendable {
    let confirmationToken: String
    enum CodingKeys: String, CodingKey { case confirmationToken = "confirmation_token" }
}
private struct UpgradePreviewBody: Codable, Sendable {
    let targetVersion: Int
    let values: [String: JSONValue]?
    enum CodingKeys: String, CodingKey {
        case targetVersion = "target_version"
        case values
    }
}
private struct UpgradeBody: Codable, Sendable {
    let targetVersion: Int
    let values: [String: JSONValue]
    let confirmationToken: String
    enum CodingKeys: String, CodingKey {
        case targetVersion = "target_version"
        case values
        case confirmationToken = "confirmation_token"
    }
}
private struct TriggerCreateBody: Codable, Sendable {
    let flowName: String
    let triggerType: String
    let triggerSpec: [String: JSONValue]
    let enabled: Bool
    let nextFireAt: String?
    enum CodingKeys: String, CodingKey {
        case enabled
        case flowName = "flow_name"
        case triggerType = "trigger_type"
        case triggerSpec = "trigger_spec"
        case nextFireAt = "next_fire_at"
    }
}
private struct TriggerPatchBody: Codable, Sendable {
    let flowName: String?
    let triggerType: String?
    let triggerSpec: [String: JSONValue]?
    let enabled: Bool?
    let nextFireAt: String?
    enum CodingKeys: String, CodingKey {
        case enabled
        case flowName = "flow_name"
        case triggerType = "trigger_type"
        case triggerSpec = "trigger_spec"
        case nextFireAt = "next_fire_at"
    }
}

public struct IngestDocument: Codable, Hashable, Sendable {
    public let id: String
    public let url: String
    public let text: String
    public let title: String
    public let canonicalURL: String?
    public let publishedAt: String?
    public let lang: String?
    public let fields: [String: JSONValue]
    public let deleted: Bool
    enum CodingKeys: String, CodingKey {
        case id, url, text, title, publishedAt = "published_at", lang, fields, deleted
        case canonicalURL = "canonical_url"
    }
    public init(id: String, url: String, text: String, title: String = "",
                canonicalURL: String? = nil, publishedAt: String? = nil,
                lang: String? = nil, fields: [String: JSONValue] = [:],
                deleted: Bool = false) {
        self.id = id; self.url = url; self.text = text; self.title = title
        self.canonicalURL = canonicalURL; self.publishedAt = publishedAt
        self.lang = lang; self.fields = fields; self.deleted = deleted
    }
}
private struct IngestBody: Codable, Sendable {
    let schemaVersion = "windex.ingest/1"
    let mode: String
    let partition: String?
    let documents: [IngestDocument]
    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case mode, partition, documents
    }
}

extension WindexClient {
    public func sources(includeArchived: Bool = false) async throws -> SourcesWire {
        try await send("GET", "/v1/sources", surface: .admin,
                       query: [.init(name: "include_archived", value: String(includeArchived))],
                       as: SourcesWire.self)
    }
    public func source(_ name: String) async throws -> SourceWire {
        try await send("GET", "/v1/sources/\(Self.escapePath(name))",
                       surface: .admin, as: SourceWire.self)
    }
    public func validateSource(_ request: SourceCreateRequest) async throws
        -> SourceValidationWire {
        try await send("POST", "/v1/sources/validate", surface: .admin,
                       body: request, as: SourceValidationWire.self)
    }
    @discardableResult
    public func createSource(_ request: SourceCreateRequest) async throws -> SourceWire {
        try await send("POST", "/v1/sources", surface: .admin,
                       body: request, as: SourceWire.self)
    }
    @discardableResult
    public func patchSource(_ name: String, title: String? = nil,
                            description: String? = nil,
                            origin: [String: JSONValue]? = nil,
                            enabled: Bool? = nil,
                            includeInAll: Bool? = nil) async throws -> SourceWire {
        try await send("PATCH", "/v1/sources/\(Self.escapePath(name))",
                       surface: .admin,
                       body: SourcePatchBody(title: title, description: description,
                                             origin: origin, enabled: enabled,
                                             includeInAll: includeInAll),
                       as: SourceWire.self)
    }
    public func validateSource(_ name: String) async throws -> SourceValidationWire {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/validate",
                       surface: .admin, as: SourceValidationWire.self)
    }
    @discardableResult
    public func archiveSource(_ name: String) async throws -> ActionWire {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/archive",
                       surface: .admin, as: ActionWire.self)
    }
    @discardableResult
    public func pauseSource(_ name: String, reason: String = "") async throws -> SourceWire {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/pause",
                       surface: .admin, body: PauseBody(reason: reason), as: SourceWire.self)
    }
    @discardableResult
    public func resumeSource(_ name: String) async throws -> SourceWire {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/resume",
                       surface: .admin, as: SourceWire.self)
    }
    public func sourceResetPreview(_ name: String) async throws
        -> Components.Schemas.ResetPreviewResponse {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/reset/preview",
                       surface: .admin, as: Components.Schemas.ResetPreviewResponse.self)
    }
    public func resetSource(_ name: String, confirmationToken: String) async throws
        -> Components.Schemas.ResetQueuedResponse {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/reset",
                       surface: .admin,
                       body: ResetBody(confirmationToken: confirmationToken),
                       as: Components.Schemas.ResetQueuedResponse.self)
    }
    public func sourceStatus(_ name: String) async throws -> SourceStatusWire {
        try await send("GET", "/v1/sources/\(Self.escapePath(name))/status",
                       surface: .admin, as: SourceStatusWire.self)
    }
    public func sourceModuleStatus(_ name: String) async throws -> SourceModuleStatusWire {
        try await send(
            "GET",
            "/v1/sources/\(Self.escapePath(name))/module-status",
            surface: .admin,
            as: SourceModuleStatusWire.self
        )
    }
    public func sourceSettings(_ name: String) async throws -> SourceSettingsWire {
        try await send("GET", "/v1/sources/\(Self.escapePath(name))/settings",
                       surface: .admin, as: SourceSettingsWire.self)
    }
    @discardableResult
    public func patchSourceSettings(_ name: String, values: [String: JSONValue],
                                    etag: String) async throws -> SourceSettingsWire {
        try await send("PATCH", "/v1/sources/\(Self.escapePath(name))/settings",
                       surface: .admin, body: SourceSettingsPatchBody(values: values),
                       headers: ["If-Match": etag], as: SourceSettingsWire.self)
    }
    @discardableResult
    public func deleteSourceSetting(_ name: String, key: String,
                                    etag: String) async throws -> SourceSettingsWire {
        try await send("DELETE",
                       "/v1/sources/\(Self.escapePath(name))/settings/\(Self.escapePath(key))",
                       surface: .admin, headers: ["If-Match": etag],
                       as: SourceSettingsWire.self)
    }
    public func previewSourceUpgrade(_ name: String, version: Int,
                                     values: [String: JSONValue]? = nil) async throws
        -> SourceUpgradePreviewWire {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/upgrade/preview",
                       surface: .admin,
                       body: UpgradePreviewBody(targetVersion: version, values: values),
                       as: SourceUpgradePreviewWire.self)
    }
    @discardableResult
    public func upgradeSource(_ name: String, version: Int,
                              values: [String: JSONValue],
                              confirmationToken: String) async throws -> SourceWire {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/upgrade",
                       surface: .admin,
                       body: UpgradeBody(targetVersion: version,
                                         values: values,
                                         confirmationToken: confirmationToken),
                       as: SourceWire.self)
    }
    public func sourceTriggers(_ name: String) async throws -> SourceTriggersWire {
        try await send("GET", "/v1/sources/\(Self.escapePath(name))/triggers",
                       surface: .admin, as: SourceTriggersWire.self)
    }
    @discardableResult
    public func createSourceTrigger(_ name: String, flow: String,
                                    type: String,
                                    enabled: Bool = true,
                                    spec: [String: JSONValue] = [:],
                                    nextFireAt: String? = nil) async throws
        -> Components.Schemas.TriggerModel {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/triggers",
                       surface: .admin,
                       body: TriggerCreateBody(flowName: flow, triggerType: type,
                                               triggerSpec: spec, enabled: enabled,
                                               nextFireAt: nextFireAt),
                       as: Components.Schemas.TriggerModel.self)
    }
    @discardableResult
    public func patchSourceTrigger(_ name: String, id: Int, flow: String? = nil,
                                   type: String? = nil,
                                   enabled: Bool? = nil,
                                   spec: [String: JSONValue]? = nil,
                                   nextFireAt: String? = nil) async throws
        -> Components.Schemas.TriggerModel {
        try await send("PATCH",
                       "/v1/sources/\(Self.escapePath(name))/triggers/\(id)",
                       surface: .admin,
                       body: TriggerPatchBody(flowName: flow, triggerType: type,
                                              triggerSpec: spec, enabled: enabled,
                                              nextFireAt: nextFireAt),
                       as: Components.Schemas.TriggerModel.self)
    }
    @discardableResult
    public func deleteSourceTrigger(_ name: String, id: Int) async throws -> ActionWire {
        try await send("DELETE",
                       "/v1/sources/\(Self.escapePath(name))/triggers/\(id)",
                       surface: .admin, as: ActionWire.self)
    }
    /// Runs the Source's current revision and current configuration.
    public func runLatestSource(_ name: String, flow: String? = nil,
                                inputs: [String: JSONValue] = [:],
                                overrides: [String: JSONValue] = [:],
                                priority: Int = 50) async throws -> QueuedRunWire {
        try await send("POST", "/v1/sources/\(Self.escapePath(name))/runs",
                       surface: .admin,
                       body: SourceRunBody(flow: flow, inputs: inputs,
                                           overrides: overrides, priority: priority),
                       as: QueuedRunWire.self)
    }

    public func ingest(_ documents: [IngestDocument], into source: String,
                       mode: String = "delta", partition: String? = nil,
                       idempotencyKey: String) async throws
        -> QueuedRunWire {
        try await send(
            "POST", "/v1/sources/\(Self.escapePath(source))/ingest",
            surface: .agentAuthenticated,
            body: IngestBody(
                mode: mode,
                partition: partition,
                documents: documents
            ),
            headers: ["Idempotency-Key": idempotencyKey],
            as: QueuedRunWire.self
        )
    }
}
