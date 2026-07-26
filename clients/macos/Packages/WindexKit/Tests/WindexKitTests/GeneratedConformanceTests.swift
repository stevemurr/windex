import Foundation
import Testing
@testable import WindexKit

@Suite("Epoch-2 generated contract")
struct GeneratedConformanceTests {
    private func decode<T: Decodable>(_ type: T.Type, _ json: String) throws -> T {
        try JSONDecoder().decode(T.self, from: Data(json.utf8))
    }

    @Test("required nullable response fields remain optional in Swift")
    func requiredNullableFieldsAreOptional() throws {
        let pipeline = try decode(PipelineWire.self, """
        {"id":1,"name":"docs","title":"Docs","description":"","builtin":false,
         "archived_at":null,"created_at":"2026-07-25T00:00:00Z",
         "updated_at":"2026-07-25T00:00:00Z","head_revision_id":null,
         "version":null,"spec_hash":null}
        """)
        #expect(pipeline.archivedAt == nil)
        #expect(pipeline.version == nil)
        #expect(pipeline.specHash == nil)

        let queued = try decode(QueuedRunWire.self, """
        {"run_id":null,"queued":false,"coalesced":null,"rerun_of":null}
        """)
        #expect(queued.runId == nil)
    }

    @Test("generated source and run models decode null lifecycle fields")
    func canonicalModelsDecode() throws {
        let source = try decode(SourceWire.self, """
        {"id":1,"name":"docs","title":"Docs","description":"","origin":{"ingress":"push"},
         "pipeline_revision_id":2,"pipeline_name":"push","pipeline_version":1,
         "pipeline_hash":"abc","search_contract_version":"1","search_name":"docs",
         "id_prefix":"docs:","collection_key":"docs","search_profile":"generic",
         "include_in_all":true,"state_namespace":"docs","enabled":true,"generation":1,
         "archived_at":null,"created_at":"2026-07-25T00:00:00Z",
         "updated_at":"2026-07-25T00:00:00Z","values":{},"values_hash":"vh",
         "paused":false,"pause_reason":"","paused_at":null,"etag":"e1","ready":true,
         "ingress":null}
        """)
        #expect(source.archivedAt == nil)
        #expect(source.pausedAt == nil)
        #expect(try source.deployment().pipeline.version == 1)
    }

    @Test("the generated source contains epoch-2 nouns only")
    func epochTwoNounsOnly() throws {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent().deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/WindexKit/Generated/Types.swift")
        let source = try String(contentsOf: url, encoding: .utf8)
        for name in ["PipelineModel", "SourceModel", "RunModel"] {
            #expect(source.contains("struct \(name)"))
        }
        #expect(!source.contains("struct Recipe"))
        #expect(!source.contains("Marketplace"))
    }
}
