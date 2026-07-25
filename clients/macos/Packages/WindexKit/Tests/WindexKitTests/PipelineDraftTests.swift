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
