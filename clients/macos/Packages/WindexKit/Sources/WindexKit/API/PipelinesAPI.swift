import Foundation

private struct PipelineCreateBody: Codable, Sendable {
    let name: String
    let title: String
    let description: String
    let spec: PipelineSpec
    let author: String
    let note: String
}

private struct PipelineRevisionBody: Codable, Sendable {
    let spec: PipelineSpec
    let parentVersion: Int?
    let parentHash: String?
    let author: String
    let note: String
    enum CodingKeys: String, CodingKey {
        case spec, author, note
        case parentVersion = "parent_version"
        case parentHash = "parent_hash"
    }
}

private struct LayoutBody: Codable, Sendable {
    let flowName: String
    let layout: [String: JSONValue]
    enum CodingKeys: String, CodingKey {
        case flowName = "flow_name"
        case layout
    }
}

private struct PipelineRunBody: Codable, Sendable {
    let version: Int?
    let flow: String?
    let inputs: [String: JSONValue]
    let parameters: [String: JSONValue]
    let priority: Int
}

extension WindexClient {
    public func pipelines(includeArchived: Bool = false) async throws -> PipelinesWire {
        try await send("GET", "/v1/pipelines", surface: .admin,
                       query: [.init(name: "include_archived", value: String(includeArchived))],
                       as: PipelinesWire.self)
    }

    public func pipeline(_ name: String) async throws -> PipelineWire {
        try await send("GET", "/v1/pipelines/\(Self.escapePath(name))",
                       surface: .admin, as: PipelineWire.self)
    }

    public func validatePipeline(_ spec: PipelineSpec) async throws -> PipelineValidationWire {
        try await send("POST", "/v1/pipelines/validate", surface: .admin,
                       body: spec, as: PipelineValidationWire.self)
    }

    @discardableResult
    public func createPipeline(name: String, title: String = "",
                               description: String = "", spec: PipelineSpec,
                               author: String = "", note: String = "") async throws
        -> PipelineWire {
        try await send("POST", "/v1/pipelines", surface: .admin,
                       body: PipelineCreateBody(name: name, title: title,
                                                description: description, spec: spec,
                                                author: author, note: note),
                       as: PipelineWire.self)
    }

    public func pipelineRevisions(_ name: String) async throws -> PipelineRevisionsWire {
        try await send("GET", "/v1/pipelines/\(Self.escapePath(name))/revisions",
                       surface: .admin, as: PipelineRevisionsWire.self)
    }

    public func pipelineRevision(_ name: String, version: Int) async throws
        -> PipelineRevisionWire {
        try await send("GET",
                       "/v1/pipelines/\(Self.escapePath(name))/revisions/\(version)",
                       surface: .admin, as: PipelineRevisionWire.self)
    }

    @discardableResult
    public func publishPipelineRevision(_ name: String, spec: PipelineSpec,
                                        parentVersion: Int?, parentHash: String?,
                                        ifMatch: String? = nil,
                                        author: String = "", note: String = "") async throws
        -> PipelineRevisionWire {
        guard parentVersion != nil || parentHash != nil || ifMatch != nil else {
            throw WindexError.preconditionRequired(
                message: "Publishing requires a parent version/hash or If-Match.")
        }
        return try await send(
            "POST", "/v1/pipelines/\(Self.escapePath(name))/revisions",
            surface: .admin,
            body: PipelineRevisionBody(spec: spec, parentVersion: parentVersion,
                                       parentHash: parentHash, author: author, note: note),
            headers: ifMatch.map { ["If-Match": $0] } ?? [:],
            as: PipelineRevisionWire.self
        )
    }

    public func pipelineTasks(_ name: String, version: Int, flow: String? = nil) async throws
        -> PipelineTaskPreviewWire {
        try await send("GET",
                       "/v1/pipelines/\(Self.escapePath(name))/revisions/\(version)/tasks",
                       surface: .admin,
                       query: flow.map { [.init(name: "flow", value: $0)] } ?? [],
                       as: PipelineTaskPreviewWire.self)
    }

    public func pipelineLayout(_ name: String, version: Int, flow: String? = nil) async throws
        -> PipelineLayoutWire {
        try await send("GET",
                       "/v1/pipelines/\(Self.escapePath(name))/revisions/\(version)/layout",
                       surface: .admin,
                       query: flow.map { [.init(name: "flow", value: $0)] } ?? [],
                       as: PipelineLayoutWire.self)
    }

    @discardableResult
    public func putPipelineLayout(_ layout: PipelineFlowLayout) async throws
        -> PipelineLayoutWire {
        guard let etag = layout.etag, !etag.isEmpty else {
            throw WindexError.preconditionRequired(
                message: "Layout writes require the layout ETag.")
        }
        return try await send(
            "PUT",
            "/v1/pipelines/\(Self.escapePath(layout.pipeline))/revisions/\(layout.version)/layout",
            surface: .admin,
            body: LayoutBody(
                flowName: layout.flow,
                layout: try layout.wirePayload()
            ),
            headers: ["If-Match": etag], as: PipelineLayoutWire.self
        )
    }

    @discardableResult
    public func archivePipeline(_ name: String) async throws -> ActionWire {
        try await send("POST", "/v1/pipelines/\(Self.escapePath(name))/archive",
                       surface: .admin, as: ActionWire.self)
    }

    /// A generic run pins a revision. Passing no version requires the current
    /// head validator so "run head" cannot silently race publication.
    public func runPipeline(_ name: String, version: Int?, headETag: String? = nil,
                            flow: String? = nil, inputs: [String: JSONValue] = [:],
                            parameters: [String: JSONValue] = [:],
                            priority: Int = 50) async throws -> QueuedRunWire {
        guard version != nil || headETag?.isEmpty == false else {
            throw WindexError.preconditionRequired(
                message: "Running Pipeline head requires If-Match.")
        }
        return try await send(
            "POST", "/v1/pipelines/\(Self.escapePath(name))/runs",
            surface: .admin,
            body: PipelineRunBody(version: version, flow: flow, inputs: inputs,
                                  parameters: parameters, priority: priority),
            headers: headETag.map { ["If-Match": $0] } ?? [:],
            as: QueuedRunWire.self
        )
    }
}
