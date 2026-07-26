import Foundation
import Testing
@testable import WindexKit

@Suite("Pipeline drafts")
struct PipelineDraftTests {
    @Test("compatible typed connections are accepted")
    func compatibleConnection() throws {
        let registry = Self.registry
        var draft = PipelineDraft(name: "docs", title: "Docs")
        let origin = try draft.addNode(module: Self.module("origin"), toFlow: "main")
        let transform = try draft.addNode(
            module: Self.module("transform"), toFlow: "main")
        let sink = try draft.addNode(module: Self.module("sink"), toFlow: "main")

        try draft.connect(
            PipelineEdge(from: .node(origin.id), to: .node(transform.id)),
            inFlow: "main",
            registry: registry)
        try draft.connect(
            PipelineEdge(from: .node(transform.id), to: .node(sink.id)),
            inFlow: "main",
            registry: registry)

        #expect(draft.flows[0].edges.count == 2)
        #expect(PipelineLocalValidator.validate(draft, registry: registry).isEmpty)
    }

    @Test("port mismatches are rejected before an Edge is added")
    func incompatibleConnection() throws {
        let registry = Self.registry
        var draft = PipelineDraft(name: "docs", title: "Docs")
        let origin = try draft.addNode(module: Self.module("origin"), toFlow: "main")
        let chunks = try draft.addNode(
            module: Self.module("chunk_sink"), toFlow: "main")

        #expect(throws: PipelineDraftError.self) {
            try draft.connect(
                PipelineEdge(from: .node(origin.id), to: .node(chunks.id)),
                inFlow: "main",
                registry: registry)
        }
        #expect(draft.flows[0].edges.isEmpty)
    }

    @Test("a connection that would create a cycle is rejected")
    func cycle() throws {
        let registry = Self.registry
        var draft = PipelineDraft(name: "docs", title: "Docs")
        let first = try draft.addNode(
            module: Self.module("transform"), toFlow: "main")
        let second = try draft.addNode(
            module: Self.module("transform"), toFlow: "main")

        try draft.connect(
            PipelineEdge(from: .node(first.id), to: .node(second.id)),
            inFlow: "main",
            registry: registry)
        #expect(throws: PipelineDraftError.self) {
            try draft.connect(
                PipelineEdge(from: .node(second.id), to: .node(first.id)),
                inFlow: "main",
                registry: registry)
        }
    }

    @Test("Module defaults materialize into Node configuration")
    func materializedDefaults() throws {
        let param = try JSONDecoder().decode(
            Param.self,
            from: Data(
                """
                {
                  "key": "limit",
                  "kind": "int",
                  "title": "Limit",
                  "default": 25
                }
                """.utf8))
        let module = PipelineModuleDescriptor(
            id: "test.configured",
            kind: "transform",
            version: "1",
            title: "Configured",
            fields: [param])
        var draft = PipelineDraft(name: "docs", title: "Docs")

        let node = try draft.addNode(module: module, toFlow: "main")

        #expect(node.config["limit"] == .literal(.int(25)))
    }

    @Test("parameter and secret references survive the wire round trip")
    func configReferences() throws {
        let values: [String: NodeConfigValue] = [
            "seed": .parameter("seed_urls"),
            "token": .secret("github"),
            "limit": .literal(.int(10)),
        ]

        let data = try JSONEncoder().encode(values)
        let decoded = try JSONDecoder().decode(
            [String: NodeConfigValue].self, from: data)

        #expect(decoded == values)
    }

    @Test("Pipeline parameter definitions preserve constraints")
    func parameterDefinition() throws {
        let definition = PipelineParameterDefinition(
            key: "batch_size",
            kind: .int,
            title: "Batch size",
            description: "Documents per batch",
            required: true,
            stage: .install,
            defaultValue: .int(64),
            choices: ["32", "64"],
            minimum: 1,
            maximum: 128,
            allowedSecrets: ["embedding-token"]
        )

        let parameter = try definition.parameter()

        #expect(parameter.key == "batch_size")
        #expect(parameter.kind == .int)
        #expect(parameter.title == "Batch size")
        #expect(parameter.required)
        #expect(parameter.stage == .install)
        #expect(parameter.defaultValue == .int(64))
        #expect(parameter.lo == 1)
        #expect(parameter.hi == 128)
        #expect(parameter.allow == ["embedding-token"])
    }

    @Test("renaming a Pipeline parameter rewrites Node bindings")
    func renameParameterBinding() throws {
        let original = try PipelineParameterDefinition(
            key: "limit",
            kind: .int
        ).parameter()
        var draft = PipelineDraft(
            name: "docs",
            title: "Docs",
            parameters: [original],
            flows: [
                PipelineFlow(
                    name: "main",
                    nodes: [
                        PipelineNode(
                            id: "fetch",
                            kind: "origin",
                            module: "test.origin",
                            config: ["limit": .parameter("limit")]
                        ),
                    ]
                ),
            ]
        )

        try draft.updateParameter(
            named: "limit",
            definition: .init(key: "page_size", kind: .int)
        )

        #expect(draft.parameters.map(\.key) == ["page_size"])
        #expect(draft.flows[0].nodes[0].config["limit"] == .parameter("page_size"))
    }

    @Test("Node binding choices respect kinds and secret allowlists")
    func nodeBindingChoices() throws {
        let field = try PipelineParameterDefinition(
            key: "token",
            kind: .string,
            allowedSecrets: ["github", "gitlab"]
        ).parameter()
        let parameters = try [
            PipelineParameterDefinition(key: "query", kind: .string).parameter(),
            PipelineParameterDefinition(key: "limit", kind: .int).parameter(),
        ]

        let options = NodeBindingOptions(
            field: field,
            parameters: parameters,
            configuredSecrets: ["other", "gitlab", "github"]
        )

        #expect(options.parameterKeys == ["query"])
        #expect(options.secretNames == ["github", "gitlab"])
        #expect(options.supports(.literal))
        #expect(options.supports(.pipelineParameter))
        #expect(options.supports(.secretReference))
    }

    @Test("renaming a Flow updates refresh references")
    func renameFlow() throws {
        var draft = PipelineDraft(
            name: "docs",
            title: "Docs",
            flows: [PipelineFlow(name: "refresh")],
            refreshFlows: ["refresh"])

        try draft.renameFlow(named: "refresh", to: "Update Docs")

        #expect(draft.flows.map(\.name) == ["update_docs"])
        #expect(draft.refreshFlows == ["update_docs"])
    }

    @Test("renaming a Node rewrites connected Edge endpoints")
    func renameNode() throws {
        let registry = Self.registry
        var draft = PipelineDraft(name: "docs", title: "Docs")
        let origin = try draft.addNode(module: Self.module("origin"), toFlow: "main")
        let sink = try draft.addNode(module: Self.module("sink"), toFlow: "main")
        try draft.connect(
            PipelineEdge(from: .node(origin.id), to: .node(sink.id)),
            inFlow: "main",
            registry: registry)

        try draft.renameNode(origin.id, to: "External docs", inFlow: "main")

        #expect(draft.flows[0].nodes.map(\.id).contains("external_docs"))
        #expect(draft.flows[0].edges[0].from == .node("external_docs"))
    }

    @Test("duplicating a Node copies configuration without copying Edges")
    func duplicateNode() throws {
        var draft = PipelineDraft(name: "docs", title: "Docs")
        let original = try draft.addNode(
            module: Self.module("transform"), toFlow: "main")

        let copy = try draft.duplicateNode(original.id, inFlow: "main")

        #expect(copy.id == "transform_copy")
        #expect(copy.config == original.config)
        #expect(draft.flows[0].edges.isEmpty)
    }

    @Test("disconnect removes only the selected Edge")
    func disconnect() throws {
        let registry = Self.registry
        var draft = PipelineDraft(name: "docs", title: "Docs")
        let origin = try draft.addNode(module: Self.module("origin"), toFlow: "main")
        let transform = try draft.addNode(
            module: Self.module("transform"), toFlow: "main")
        let edge = PipelineEdge(from: .node(origin.id), to: .node(transform.id))
        try draft.connect(edge, inFlow: "main", registry: registry)

        try draft.disconnect(edge, inFlow: "main")

        #expect(draft.flows[0].edges.isEmpty)
    }

    @Test("canvas layout is absent from semantic Pipeline JSON")
    func layoutIsSeparate() throws {
        let spec = PipelineDraft(name: "docs", title: "Docs").spec
        let encoded = String(
            decoding: try JSONEncoder().encode(spec),
            as: UTF8.self)

        #expect(!encoded.contains("positions"))
        #expect(!encoded.contains("annotations"))
    }

    @Test("Source-only Module roles gate generic Runs per Flow")
    func genericRunCapability() throws {
        let registry = PipelineRegistry(
            version: 1,
            portTypes: [],
            kinds: [],
            modules: [
                .init(
                    id: "pure.transform",
                    kind: "transform",
                    version: "1",
                    title: "Pure transform"
                ),
                .init(
                    id: "time.windows",
                    kind: "discover",
                    version: "1",
                    title: "Time windows",
                    contractRoles: ["ingress.pull", "state.read"]
                ),
                .init(
                    id: "ledger.stage",
                    kind: "load",
                    version: "1",
                    title: "Stage documents",
                    contractRoles: [
                        "document.staging",
                        "document.identity",
                        "document.provenance",
                    ]
                ),
            ]
        )
        let pure = PipelineFlow(
            name: "pure",
            nodes: [
                .init(
                    id: "transform",
                    kind: "transform",
                    module: "pure.transform"
                ),
            ]
        )
        let sourceBound = PipelineFlow(
            name: "harvest",
            nodes: [
                .init(
                    id: "windows",
                    kind: "discover",
                    module: "time.windows"
                ),
                .init(
                    id: "stage",
                    kind: "load",
                    module: "ledger.stage"
                ),
                .init(
                    id: "stage_again",
                    kind: "load",
                    module: "ledger.stage"
                ),
            ]
        )

        #expect(registry.genericRunCapability(for: pure) == .runnable)
        let capability = registry.genericRunCapability(for: sourceBound)
        #expect(!capability.canRun)
        #expect(capability.requiresSource)
        #expect(capability.blockers.map(\.moduleID) == [
            "ledger.stage",
            "time.windows",
        ])
        #expect(capability.blockers[0].roles == [
            "document.identity",
            "document.provenance",
            "document.staging",
        ])
        #expect(capability.explanation.contains("ledger.stage, time.windows"))
    }

    private static let registry = PipelineRegistry(
        version: 1,
        portTypes: [
            .init(name: "documents", title: "Documents"),
            .init(name: "chunks", title: "Chunks"),
        ],
        kinds: [
            .init(
                id: "origin", title: "Origin", description: "",
                inputType: nil, outputType: "documents", stateful: false),
            .init(
                id: "transform", title: "Transform", description: "",
                inputType: "documents", outputType: "documents", stateful: false),
            .init(
                id: "sink", title: "Sink", description: "",
                inputType: "documents", outputType: nil, stateful: true),
            .init(
                id: "chunk_sink", title: "Chunk sink", description: "",
                inputType: "chunks", outputType: nil, stateful: true),
        ],
        modules: [
            module("origin"),
            module("transform"),
            module("sink"),
            module("chunk_sink"),
        ])

    private static func module(_ id: String) -> PipelineModuleDescriptor {
        PipelineModuleDescriptor(
            id: "test.\(id)",
            kind: id,
            version: "1",
            title: id.capitalized)
    }
}

@Suite("Source deployments")
struct SourceDeploymentTests {
    @Test("configuration readiness is explicit")
    func readiness() {
        let ready = SourceConfiguration()
        let missing = SourceConfiguration(missingRequired: ["seed_urls"])

        #expect(ready.isReady)
        #expect(!missing.isReady)
    }
}

@Suite("Pipeline draft recovery")
struct PipelineDraftRecoveryTests {
    @Test("the newest unpublished draft round-trips independently of layout")
    func roundTrip() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = PipelineDraftRecoveryStore(directory: directory)
        let older = RecoveredPipelineDraft(
            draft: PipelineDraft(name: "first", title: "First"),
            selectedFlow: "main",
            updatedAt: Date(timeIntervalSince1970: 1))
        let newest = RecoveredPipelineDraft(
            draft: PipelineDraft(name: "second", title: "Second"),
            baseVersion: 4,
            baseHash: "abc",
            selectedFlow: "main",
            positions: ["fetch": .init(x: 10, y: 20)],
            updatedAt: Date(timeIntervalSince1970: 2))

        try await store.save(older)
        try await store.save(newest)

        #expect(try await store.latest() == newest)
        #expect(try await store.load(id: newest.id) == newest)
        try await store.discard(id: newest.id)
        #expect(try await store.load(id: newest.id) == nil)
    }
}
