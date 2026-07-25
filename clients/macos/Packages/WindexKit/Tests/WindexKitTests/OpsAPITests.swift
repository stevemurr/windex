import Foundation
import Testing
@testable import WindexKit

/// Fixtures below are captured verbatim from a running backend, so these check
/// the client against what the server actually sends.
@Suite("Ops API")
struct OpsAPITests {

    private func makeServer() throws -> MockWindexServer {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/loops", json: """
            {"watchdog_running":false,"indexing_paused":true,
             "loops":[{"source":"ccnews","enabled":true,"running":true,"state":"up",
                       "pids":[],"ingest_enabled":true,"log":"embed-loop"},
                      {"source":"gh","enabled":true,"running":true,"state":"up",
                       "pids":[],"ingest_enabled":true,"log":"gh-embed"}]}
            """)
        server.on("GET /admin/v1/freshness", json: """
            [{"source":"ccnews","indexed":1822983,"pending":578066,
              "last_embed_ts":null,"last_update_ts":1784689216.0},
             {"source":"wiki","indexed":2408435,"pending":4800208,
              "last_embed_ts":null,"last_update_ts":1784865055.0}]
            """)
        server.on("GET /admin/v1/activity", json: """
            [{"name":"refresh","label":"Refresh sweep","group":"action",
              "running":false,"last_ts":null,"error":false},
             {"name":"embed-loop","label":"loop · ccnews","group":"loop",
              "running":false,"last_ts":null,"error":true}]
            """)
        server.on("GET /admin/v1/workers", json: """
            {"active":false,"stage":"paused"}
            """)
        server.on("GET /admin/v1/schedule", json: """
            [{"name":"daily","kind":"command","target":"daily","hour":2,"minute":15,
              "weekday":null,"enabled":true,"running":false,
              "last_run":"2026-07-24T02:15:52.517640+00:00",
              "last_run_ts":1784859352.51764,
              "label":"Daily freshness","cadence":"daily · 02:15"}]
            """)
        server.on("GET /admin/v1/logs", json: """
            [{"name":"server","title":"Server","description":"REST API",
              "category":"server","kind":"file","size":19752,"mtime":1784935937,
              "available":true},
             {"name":"watchdog","title":"Watchdog","description":"Health monitor",
              "category":"server","kind":"file","size":null,"mtime":null,
              "available":false}]
            """)
        server.on("GET /admin/v1/timeseries", json: """
            [{"t":"2026-07-24T22:33:00+00:00","ingested":0,"docs":1584,"mb":0.0},
             {"t":"2026-07-24T22:34:00+00:00","ingested":0,"docs":1552,"mb":0.0}]
            """)
        return server
    }

    @Test("loops decode with the global pause flag")
    func loops() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let state = try await client.loops()

        #expect(state.indexingPaused == true)
        #expect(state.watchdogRunning == false)
        #expect(state.loops?.count == 2)
        let first = try #require(state.loops?.first)
        #expect(first.source == "ccnews")
        #expect(first.state == "up")
        #expect(first.ingestEnabled == true)
        #expect(first.log == "embed-loop")
        #expect(server.lastRequest?.header("Authorization") == "Bearer t")
    }

    @Test("freshness carries millions without loss")
    func freshness() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let rows = try await client.freshness()

        #expect(rows.count == 2)
        let wiki = try #require(rows.first { $0.source == "wiki" })
        #expect(wiki.indexed == 2_408_435)
        #expect(wiki.pending == 4_800_208)
        // Null timestamps are common — a source that has never run has no last
        // embed. They must decode as nil, not fail the row.
        #expect(wiki.lastEmbedTs == nil)
        #expect(wiki.lastUpdateTs != nil)
    }

    /// `error` is a bool flag, not a message — this was a server model bug the
    /// generated types caught, so it is worth pinning.
    @Test("activity error is a flag")
    func activity() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let items = try await client.activity()

        #expect(items.count == 2)
        #expect(items[0].group == "action")
        #expect(items[0].error == false)
        #expect(items[1].error == true)
        #expect(items[1].group == "loop")
    }

    @Test("workers active is a bool")
    func workers() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let state = try await client.workers()
        #expect(state.active == false)
        #expect(state.stage == "paused")
    }

    @Test("schedule entries carry the server's rendered cadence")
    func schedule() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let entries = try await client.schedule()

        let daily = try #require(entries.first)
        #expect(daily.name == "daily")
        #expect(daily.hour == 2)
        #expect(daily.minute == 15)
        #expect(daily.weekday == nil)          // daily, not weekly
        #expect(daily.cadence == "daily · 02:15")
        #expect(daily.label == "Daily freshness")
    }

    /// Most logs are unavailable most of the time — that is the common case, so
    /// an unavailable row must decode cleanly rather than trip on null size.
    @Test("unavailable logs decode")
    func logs() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let sources = try await client.logs()

        #expect(sources.count == 2)
        #expect(sources[0].available == true)
        #expect(sources[0].size == 19752)
        #expect(sources[1].available == false)
        #expect(sources[1].size == nil)
        #expect(sources[1].mtime == nil)
    }

    @Test("timeseries t is an ISO string")
    func timeseries() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let points = try await client.timeseries(minutes: 60)

        #expect(points.count == 2)
        #expect(points[0].t == "2026-07-24T22:33:00+00:00")
        #expect(points[0].docs == 1584)
        #expect(server.lastRequest?.query["minutes"] == "60")
    }

    /// The server 422s an out-of-range window. A UI slider that allows more
    /// should hit the cap, not an error dialog.
    @Test("query windows are clamped to the server's documented bounds")
    func queryBoundsAreClamped() async throws {
        let server = try makeServer()
        server.on("GET /admin/v1/recent", json: "[]")
        server.on("GET /admin/v1/logs/server", json: """
            {"name":"server","available":true,"truncated":false,"lines":[]}
            """)
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")

        _ = try await client.timeseries(minutes: 99_999)     // max 1440
        #expect(server.lastRequest?.query["minutes"] == "1440")

        _ = try await client.timeseries(minutes: 1)          // min 5
        #expect(server.lastRequest?.query["minutes"] == "5")

        _ = try await client.recent(limit: 5_000)            // max 100
        #expect(server.lastRequest?.query["limit"] == "100")

        _ = try await client.logTail(name: "server", lines: 99_999)  // max 2000
        #expect(server.lastRequest?.query["lines"] == "2000")
    }

    @Test("log tail sends its filters")
    func logTailFilters() async throws {
        let server = try makeServer()
        server.on("GET /admin/v1/logs/embed-loop", json: """
            {"name":"embed-loop","available":true,"truncated":true,
             "lines":["one","two"]}
            """)
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let tail = try await client.logTail(name: "embed-loop", lines: 500,
                                            grep: "timeout", level: .error)

        #expect(tail.available == true)
        #expect(tail.truncated == true)
        #expect(tail.lines?.count == 2)

        let sent = try #require(server.lastRequest)
        #expect(sent.query["lines"] == "500")
        #expect(sent.query["grep"] == "timeout")
        #expect(sent.query["level"] == "error")
    }

    /// `/v1/stats` is declared on the main app, not the ops router — so unlike
    /// every other ops read it is open and must not carry the token.
    @Test("stats is on the open agent surface")
    func statsIsAgentSurface() async throws {
        let server = try makeServer()
        server.on("GET /v1/stats", json: """
            {"documents":{"hn":{"embedded":3054752}},
             "activity":{"control":"paused","docs_per_min":0.0,
                         "embed_breaker":{"state":"closed"}},
             "vectors":{}}
            """)
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "s3cret")
        let stats = try await client.stats()

        #expect(stats.objectValue?["documents"]?
            .objectValue?["hn"]?.objectValue?["embedded"]?.intValue == 3_054_752)
        #expect(stats.objectValue?["activity"]?
            .objectValue?["control"]?.stringValue == "paused")

        let sent = try #require(server.lastRequest)
        #expect(sent.path == "/v1/stats", "no /admin prefix")
        #expect(sent.header("Authorization") == nil, "must not leak the token")
    }
}

@Suite("Control API")
struct ControlAPITests {

    private func makeServer() throws -> MockWindexServer {
        let server = try MockWindexServer()
        server.on("POST /admin/v1/jobs/ccnews-sync/start", json: """
            {"started":"ccnews-sync","pid":4412}
            """)
        server.on("POST /admin/v1/loops/ccnews", json: """
            {"source":"ccnews","enabled":false,"state":"disabled"}
            """)
        server.on("POST /admin/v1/control/pause", json: """
            {"indexing":"paused"}
            """)
        server.on("PUT /admin/v1/schedule/daily", json: """
            {"name":"daily","kind":"command","target":"daily","hour":4,"minute":30,
             "weekday":null,"enabled":true,"running":false,
             "label":"Daily freshness","cadence":"daily · 04:30"}
            """)
        return server
    }

    @Test("starting a job sends its arguments as the body")
    func startJobSendsArguments() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let result = try await client.startJob("ccnews-sync",
                                               arguments: ["days": .int(30)])

        #expect(result.started == "ccnews-sync")
        #expect(result.pid == 4412)
        let sent = try #require(server.lastRequest)
        #expect(sent.method == "POST")
        #expect(sent.body.contains("\"days\":30"))
        #expect(sent.header("Authorization") == "Bearer t")
    }

    /// Starting an already-running job is a 409, which is a distinct condition
    /// from a failure — the UI should say "already running", not "error".
    @Test("a job already running is a 409")
    func alreadyRunningIs409() async throws {
        let server = try makeServer()
        server.on("POST /admin/v1/jobs/busy/start") { _ in
            .detail("job already running: busy", status: 409)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        do {
            _ = try await client.startJob("busy")
            Issue.record("expected a throw")
        } catch let error as WindexError {
            guard case .http(let status, let message) = error else {
                Issue.record("expected .http, got \(error)")
                return
            }
            #expect(status == 409)
            #expect(message.contains("already running"))
        }
    }

    @Test("loop and control toggles send desired state")
    func togglesSendState() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")

        _ = try await client.setLoop(source: "ccnews", enabled: false)
        var sent = try #require(server.lastRequest)
        #expect(sent.path == "/admin/v1/loops/ccnews")
        #expect(sent.body.contains("\"enabled\":false"))

        let control = try await client.setIndexing(.pause)
        #expect(control.indexing == "paused")
        sent = try #require(server.lastRequest)
        #expect(sent.path == "/admin/v1/control/pause")
    }

    /// Upsert preserves unspecified fields server-side, so the client must send
    /// only what changed — a nil field that went out as null would clear it.
    @Test("schedule upsert omits unset fields")
    func scheduleUpsertOmitsUnsetFields() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let entry = try await client.upsertSchedule(name: "daily", hour: 4, minute: 30)

        #expect(entry.hour == 4)
        #expect(entry.cadence == "daily · 04:30")

        let sent = try #require(server.lastRequest)
        #expect(sent.method == "PUT")
        #expect(sent.body.contains("\"hour\":4"))
        #expect(sent.body.contains("\"minute\":30"))
        #expect(!sent.body.contains("weekday"), "unset fields must not be sent")
        #expect(!sent.body.contains("kind"))
    }
}
