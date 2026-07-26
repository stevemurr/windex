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

    @Test("generic Pipeline dry-runs send dry_run in the queued Run body")
    func genericPipelineDryRun() async throws {
        let server = try MockWindexServer()
        server.on("POST /admin/v1/pipelines/push/runs") { request in
            #expect(request.body.contains("\"version\":4"))
            #expect(request.body.contains("\"dry_run\":true"))
            #expect(request.body.contains("\"parameters\":{\"batch\":20}"))
            return .json(
                #"{"run_id":13,"queued":true,"coalesced":false,"rerun_of":null}"#,
                status: 202
            )
        }
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")

        let result = try await client.runPipeline(
            "push",
            version: 4,
            parameters: ["batch": 20],
            dryRun: true
        )

        #expect(result.runId == 13)
        #expect(result.queued)
    }

    @Test("edited Source upgrade preview and confirmation send exact values")
    func editedSourceUpgradeBodies() async throws {
        let server = try MockWindexServer()
        server.on("POST /admin/v1/sources/docs/upgrade/preview") { request in
            #expect(request.body.contains("\"target_version\":5"))
            #expect(request.body.contains("\"values\":{"))
            #expect(request.body.contains("\"batch\":24"))
            #expect(request.body.contains("\"mode\":\"safe\""))
            return .detail("body checked", status: 422)
        }
        server.on("POST /admin/v1/sources/docs/upgrade") { request in
            #expect(request.body.contains("\"target_version\":5"))
            #expect(request.body.contains("\"values\":{"))
            #expect(request.body.contains("\"batch\":24"))
            #expect(request.body.contains("\"mode\":\"safe\""))
            #expect(request.body.contains("\"confirmation_token\":\"candidate-token\""))
            return .detail("body checked", status: 409)
        }
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")
        let candidate: [String: JSONValue] = [
            "batch": 24,
            "mode": "safe",
        ]

        await #expect(throws: WindexError.self) {
            _ = try await client.previewSourceUpgrade(
                "docs",
                version: 5,
                values: candidate
            )
        }
        await #expect(throws: WindexError.self) {
            _ = try await client.upgradeSource(
                "docs",
                version: 5,
                values: candidate,
                confirmationToken: "candidate-token"
            )
        }
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

    @Test("event triggers can be created and edited with a new Flow and spec")
    func eventTriggerWrites() async throws {
        let server = try MockWindexServer()
        let trigger = """
            {
              "id":12,
              "flow_name":"refresh",
              "trigger_type":"event",
              "trigger_spec":{"event":"document.changed","source":"upstream"},
              "enabled":true,
              "next_fire_at":null,
              "last_fired_at":null,
              "last_run_id":null
            }
            """
        server.on("POST /admin/v1/sources/docs/triggers") { request in
            #expect(request.body.contains("\"trigger_type\":\"event\""))
            #expect(request.body.contains("\"event\":\"document.changed\""))
            return .json(trigger)
        }
        server.on("PATCH /admin/v1/sources/docs/triggers/12") { request in
            #expect(request.body.contains("\"flow_name\":\"rebuild\""))
            #expect(request.body.contains("\"source\":\"archive\""))
            #expect(request.body.contains("\"enabled\":false"))
            return .json(trigger)
        }
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL, token: "token")

        _ = try await client.createSourceTrigger(
            "docs",
            flow: "refresh",
            type: "event",
            spec: [
                "event": .string("document.changed"),
                "source": .string("upstream"),
            ]
        )
        _ = try await client.patchSourceTrigger(
            "docs",
            id: 12,
            flow: "rebuild",
            type: "event",
            enabled: false,
            spec: [
                "event": .string("document.changed"),
                "source": .string("archive"),
            ]
        )
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
