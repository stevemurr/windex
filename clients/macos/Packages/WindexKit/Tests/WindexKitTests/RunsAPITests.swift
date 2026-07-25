import Foundation
import Testing
@testable import WindexKit

@Suite("Generic runs API")
struct RunsAPITests {

    private let runJSON = """
        {"id":42,"recipe":"gh","recipe_version":2,"source":"github",
         "state":"running","trigger":"manual","trigger_by":"admin API",
         "params":{"flow":"hydrate"},"mode":"run","priority":50,
         "cancel_requested":false,"queued_at":"2026-07-25T05:00:00Z",
         "updated_at":"2026-07-25T05:00:01Z","progress":{},"stats":{},
         "tasks":[
           {"id":101,"run_id":42,"node":"repos","module":"state.repos_pending",
            "state":"succeeded","kind":"discover","lane":"io",
            "units_total":40,"units_done":40,"units_failed":0},
           {"id":102,"run_id":42,"node":"hydrate","module":"github.graphql_batch",
            "state":"running","kind":"fetch","lane":"net",
            "units_total":40,"units_done":12,"units_failed":1}
         ]}
        """

    @Test("list and detail use the generic admin surface")
    func listAndDetail() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/runs", json: "{\"runs\":[\(runJSON)]}")
        server.on("GET /admin/v1/runs/42", json: runJSON)
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let runs = try await client.runs(
            recipe: "gh", state: "running", beforeID: 100, limit: 20)
        let detail = try await client.run(id: 42)

        #expect(runs.map(\.id) == [42])
        #expect(detail.tasks?.map(\.node) == ["repos", "hydrate"])
        #expect(detail.tasks?[1].unitsDone == 12)
        #expect(server.requests[0].path == "/admin/v1/runs")
        #expect(server.requests[0].query["recipe"] == "gh")
        #expect(server.requests[0].query["state"] == "running")
        #expect(server.requests[0].query["before_id"] == "100")
        #expect(server.requests[0].query["limit"] == "20")
        #expect(server.requests[0].header("Authorization") == "Bearer secret")
    }

    @Test("create freezes caller parameters on the server")
    func create() async throws {
        let server = try MockWindexServer()
        server.on("POST /admin/v1/runs") { request in
            #expect(request.body.contains("\"recipe\":\"crawl\""))
            #expect(request.body.contains("\"max_pages\":500"))
            #expect(request.body.contains("\"mode\":\"dry_run\""))
            return .json("{\"run_id\":42,\"queued\":true,\"coalesced\":false}",
                         status: 202)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let queued = try await client.createRun(
            recipe: "crawl",
            params: ["max_pages": .int(500)],
            dryRun: true)

        #expect(queued.runId == 42)
        #expect(queued.queued == true)
        #expect(queued.coalesced == false)
    }

    @Test("events page and stream decode monotonic updates")
    func events() async throws {
        let event = """
            {"seq":8,"run_id":42,"task_id":102,"ts":"2026-07-25T05:00:02Z",
             "level":"info","event":"task.progress","message":"12 / 40",
             "data":{"done":12}}
            """
        let server = try MockWindexServer()
        server.on("GET /admin/v1/runs/42/events",
                  json: "{\"events\":[\(event)],\"next_cursor\":8}")
        let streamRun = runJSON.replacingOccurrences(of: "\n", with: "")
        let streamEvent = event.replacingOccurrences(of: "\n", with: "")
        server.on("GET /admin/v1/runs/42/events/stream") { _ in
            .sse([
                "event: run\ndata: \(streamRun)\n\n",
                "event: events\ndata: [\(streamEvent)]\n\n",
                "event: end\ndata: {\"cursor\":8}\n\n",
            ])
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let page = try await client.runEvents(id: 42, after: 3)
        #expect(page.first?.seq == 8)
        #expect(page.first?.data["done"] == .int(12))

        let updates = try await client.runUpdates(id: 42, after: 3)
        var names: [String] = []
        for try await update in updates {
            switch update {
            case .run(let run): names.append("run:\(run.state)")
            case .events(let rows): names.append("events:\(rows.count)")
            case .end(let cursor): names.append("end:\(cursor)")
            case .serverError(let message): names.append("error:\(message)")
            case .unknown(let name, _): names.append("unknown:\(name)")
            }
        }
        #expect(names == ["run:running", "events:1", "end:8"])
    }

    @Test("cancel remains a normal typed action")
    func cancel() async throws {
        let server = try MockWindexServer()
        server.on("POST /admin/v1/runs/42/cancel",
                  json: "{\"ok\":true,\"run_id\":42}")
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let result = try await client.cancelRun(id: 42)
        #expect(result.ok == true)
    }
}
