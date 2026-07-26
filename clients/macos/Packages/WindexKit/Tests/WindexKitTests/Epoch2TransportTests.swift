import Foundation
import Testing
@testable import WindexKit

@Suite("Epoch-2 transports")
struct Epoch2TransportTests {
    private var spec: PipelineSpec {
        PipelineSpec(
            title: "Push",
            flows: [
                PipelineFlow(
                    name: "receive",
                    inputs: [.init(name: "documents", type: "DocumentBatch")],
                    nodes: [.init(id: "receive", kind: "receive", module: "push.docs")],
                    edges: [.init(from: .input("documents"), to: .node("receive"))]
                )
            ],
            refreshFlows: []
        )
    }

    @Test("publication sends semantic parent concurrency data")
    func publicationPrecondition() async throws {
        let server = try MockWindexServer()
        server.on("POST /admin/v1/pipelines/push/revisions") { request in
            #expect(request.header("If-Match") == "head-etag")
            #expect(request.body.contains("\"parent_version\":3"))
            #expect(request.body.contains("\"parent_hash\":\"old-hash\""))
            #expect(request.body.contains("\"flows\":{\"receive\""))
            return .detail("stale parent", status: 412)
        }
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")
        do {
            _ = try await client.publishPipelineRevision(
                "push", spec: spec, parentVersion: 3,
                parentHash: "old-hash", ifMatch: "head-etag")
            Issue.record("expected stale write")
        } catch let error as WindexError {
            guard case .preconditionFailed = error else {
                Issue.record("wrong error: \(error)"); return
            }
        }
    }

    @Test("layout and Source settings use independent ETags")
    func independentETags() async throws {
        let server = try MockWindexServer()
        server.on("PUT /admin/v1/pipelines/push/revisions/4/layout") { request in
            #expect(request.header("If-Match") == "layout-v2")
            #expect(request.body.contains("\"nodes\""))
            #expect(!request.body.contains("\"positions\""))
            #expect(request.body.contains("\"groups\":[]"))
            #expect(request.body.contains("\"annotations\":[]"))
            return .detail("stale layout", status: 412)
        }
        server.on("PATCH /admin/v1/sources/docs/settings") { request in
            #expect(request.header("If-Match") == "settings-v7")
            return .detail("stale settings", status: 412)
        }
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")
        let layout = PipelineFlowLayout(
            pipeline: "push", version: 4, flow: "receive",
            positions: ["receive": .init(x: 10, y: 20)], etag: "layout-v2")
        await #expect(throws: WindexError.self) {
            _ = try await client.putPipelineLayout(layout)
        }
        await #expect(throws: WindexError.self) {
            _ = try await client.patchSourceSettings(
                "docs", values: ["batch": 10], etag: "settings-v7")
        }
    }

    @Test("Run list limits are bounded to the backend maximum")
    func boundedRunLimit() async throws {
        let server = try MockWindexServer()
        let response = #"{"runs":[]}"#
        server.on("GET /admin/v1/runs", json: response)
        server.on("GET /admin/v1/sources/docs/runs", json: response)
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")
        _ = try await client.runs(limit: 250)
        _ = try await client.sourceRuns("docs", limit: 999)
        #expect(server.requests.allSatisfy { request in
            request.query["limit"] == "200"
        })
    }

    @Test("generic head runs require If-Match while pinned runs do not")
    func genericRunPrecondition() async throws {
        let server = try MockWindexServer()
        server.on("POST /admin/v1/pipelines/push/runs") { request in
            #expect(request.header("If-Match") == nil)
            return .json(#"{"run_id":9,"queued":true,"coalesced":false,"rerun_of":null}"#,
                         status: 202)
        }
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")
        await #expect(throws: WindexError.self) {
            _ = try await client.runPipeline("push", version: nil)
        }
        let result = try await client.runPipeline("push", version: 4)
        #expect(result.runId == 9)
    }

    @Test("historic re-run and Run latest use different routes")
    func distinctRunActions() async throws {
        let server = try MockWindexServer()
        let queued = #"{"run_id":10,"queued":true,"coalesced":false,"rerun_of":9}"#
        server.on("POST /admin/v1/runs/9/rerun", json: queued, status: 202)
        server.on("POST /admin/v1/sources/docs/runs", json: queued, status: 202)
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")
        _ = try await client.rerunFrozen(9)
        _ = try await client.runLatestSource("docs")
        #expect(server.requests.map(\.path) == [
            "/admin/v1/runs/9/rerun",
            "/admin/v1/sources/docs/runs"
        ])
    }

    @Test("ingest uses the agent route with token and idempotency key")
    func ingest() async throws {
        let server = try MockWindexServer()
        server.on("POST /v1/sources/docs/ingest") { request in
            #expect(request.header("Authorization") == "Bearer token")
            #expect(request.header("Idempotency-Key") == "batch-0001")
            #expect(request.body.contains("\"schema_version\""))
            #expect(request.body.contains("windex.ingest"))
            return .json(#"{"run_id":11,"queued":true,"coalesced":false,"rerun_of":null}"#,
                         status: 202)
        }
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")
        let result = try await client.ingest(
            [.init(id: "one", url: "https://example.test/one", text: "hello")],
            into: "docs", idempotencyKey: "batch-0001")
        #expect(result.runId == 11)
    }

    @Test("409, 412, and 428 remain distinct")
    func distinctConflictErrors() async throws {
        for (status, route) in [(409, "conflict"), (412, "stale"), (428, "required")] {
            let server = try MockWindexServer()
            server.on("GET /admin/v1/overview") { _ in .detail(route, status: status) }
            try await server.start()
            let client = WindexClient(baseURL: server.baseURL, token: "token")
            do {
                _ = try await client.overview()
                Issue.record("expected \(status)")
            } catch let error as WindexError {
                switch (status, error) {
                case (409, .conflict), (412, .preconditionFailed),
                     (428, .preconditionRequired): break
                default: Issue.record("wrong mapping for \(status): \(error)")
                }
            }
            server.stop()
        }
    }
}
