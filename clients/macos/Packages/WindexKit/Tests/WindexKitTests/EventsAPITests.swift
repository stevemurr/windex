import Foundation
import Testing
@testable import WindexKit

@Suite("Dashboard events")
struct EventsAPITests {

    /// The six feeds the server multiplexes, in a plausible tick order.
    private static let stream: [String] = [
        "event: stats\ndata: {\"activity\":{\"control\":\"paused\",\"docs_per_min\":1584.0}}\n\n",
        "event: workers\ndata: {\"active\":false,\"stage\":\"paused\"}\n\n",
        "event: jobs\ndata: [{\"name\":\"ccnews-sync\",\"running\":false,\"params\":{}}]\n\n",
        ": keep-alive\n\n",
        "event: logsizes\ndata: [{\"name\":\"server\",\"available\":true,\"size\":19752}]\n\n",
        "event: timeseries\ndata: [{\"t\":\"2026-07-24T22:33:00+00:00\",\"docs\":1584}]\n\n",
        "event: recent\ndata: [{\"id\":\"hn:1\",\"source\":\"hn\",\"title\":\"a post\"}]\n\n",
    ]

    @Test("every feed decodes into its own case")
    func allFeedsDecode() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/events") { _ in .sse(Self.stream) }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        var received: [DashboardEvent] = []
        for try await event in try await client.dashboardEvents() {
            received.append(event)
        }

        #expect(received.count == 6, "the keep-alive comment must not yield an event")
        #expect(received.map(\.name)
            == ["stats", "workers", "jobs", "logsizes", "timeseries", "recent"])

        guard case .stats(let stats) = received[0] else {
            Issue.record("expected .stats"); return
        }
        #expect(stats.objectValue?["activity"]?
            .objectValue?["control"]?.stringValue == "paused")

        guard case .workers(let workers) = received[1] else {
            Issue.record("expected .workers"); return
        }
        #expect(workers.stage == "paused")

        guard case .jobs(let jobs) = received[2] else {
            Issue.record("expected .jobs"); return
        }
        #expect(jobs.first?.name == "ccnews-sync")

        guard case .timeseries(let series) = received[4] else {
            Issue.record("expected .timeseries"); return
        }
        #expect(series.first?.docs == 1584)

        guard case .recent(let recent) = received[5] else {
            Issue.record("expected .recent"); return
        }
        #expect(recent.first?.id == "hn:1")
    }

    /// Losing one tick is recoverable — the next arrives in two seconds. Tearing
    /// down the stream turns a transient server hiccup into a dead dashboard, so
    /// a malformed payload must be skipped, not thrown.
    @Test("a malformed payload skips one event without killing the stream")
    func malformedPayloadDoesNotKillTheStream() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/events") { _ in
            .sse([
                "event: workers\ndata: {\"active\":false,\"stage\":\"ok\"}\n\n",
                "event: jobs\ndata: {not json at all\n\n",
                "event: workers\ndata: {\"active\":true,\"stage\":\"running\"}\n\n",
            ])
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        var names: [String] = []
        for try await event in try await client.dashboardEvents() {
            names.append(event.name)
        }

        #expect(names == ["workers", "workers"], "the bad jobs frame is skipped")
    }

    /// A feed this client doesn't model is carried rather than dropped, so a
    /// server that adds one is visible in a debug view before it's supported.
    @Test("an unmodelled feed surfaces as .unknown")
    func unknownFeedIsCarried() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/events") { _ in
            .sse(["event: quotas\ndata: {\"left\":5}\n\n"])
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        var events: [DashboardEvent] = []
        for try await event in try await client.dashboardEvents() {
            events.append(event)
        }

        guard case .unknown(let name, let data) = events.first else {
            Issue.record("expected .unknown, got \(String(describing: events.first))")
            return
        }
        #expect(name == "quotas")
        #expect(data == "{\"left\":5}")
    }

    @Test("the stream is authenticated and bounded by ticks")
    func streamIsAuthenticated() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/events") { _ in
            .sse(["event: workers\ndata: {\"active\":false}\n\n"])
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "s3cret")
        for try await _ in try await client.dashboardEvents(ticks: 1) {}

        let sent = try #require(server.lastRequest)
        #expect(sent.path == "/admin/v1/events")
        #expect(sent.query["ticks"] == "1")
        #expect(sent.header("Authorization") == "Bearer s3cret")
        #expect(sent.header("Accept") == "text/event-stream")
    }
}

@Suite("Registry cache")
struct RegistryCacheTests {

    private static let registryJSON = """
        {"registry_version":7,"port_types":{"doc":{"label":"Document"}},
         "kinds":[{"id":"source","inputs":[],"outputs":["doc"]}],
         "modules":[{"id":"http.get","kind":"transform"}],
         "always_before_load":["dedup"]}
        """

    private func tempFile() -> URL {
        URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("windex-registry-test-\(UUID().uuidString).json")
    }

    @Test("first load fetches and stores the ETag")
    func firstLoadStores() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/registry") { _ in
            var response = MockWindexServer.Response.json(Self.registryJSON)
            response.headers["ETag"] = "W/\"registry-7\""
            return response
        }
        try await server.start()
        defer { server.stop() }

        let file = tempFile()
        defer { try? FileManager.default.removeItem(at: file) }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let cache = RegistryCache(client: client, fileURL: file)
        let registry = try await cache.load()

        #expect(registry.registryVersion == 7)
        #expect(FileManager.default.fileExists(atPath: file.path),
                "the copy must survive a process restart")

        // The first request carries no validator; there was nothing cached.
        #expect(server.requests.first?.header("If-None-Match") == nil)
    }

    /// The point of the ETag: an unchanged palette costs a round trip, not a
    /// payload.
    @Test("a second load revalidates and accepts 304")
    func revalidates() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/registry") { request in
            if request.header("If-None-Match") == "W/\"registry-7\"" {
                return MockWindexServer.Response(status: 304, headers: [:], body: Data())
            }
            var response = MockWindexServer.Response.json(Self.registryJSON)
            response.headers["ETag"] = "W/\"registry-7\""
            return response
        }
        try await server.start()
        defer { server.stop() }

        let file = tempFile()
        defer { try? FileManager.default.removeItem(at: file) }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let cache = RegistryCache(client: client, fileURL: file)

        _ = try await cache.load()
        let second = try await cache.load()

        #expect(second.registryVersion == 7)
        #expect(server.requests.count == 2)
        #expect(server.requests[1].header("If-None-Match") == "W/\"registry-7\"")
        await #expect(!cache.wasStale)
    }

    /// A stale palette beats an editor that cannot open. The backend blinking
    /// during a restart is a normal event on a self-hosted box.
    @Test("a transport failure falls back to the cached copy and says so")
    func fallsBackWhenBackendIsDown() async throws {
        let file = tempFile()
        defer { try? FileManager.default.removeItem(at: file) }

        // Populate the cache from a live server, then take it away.
        let server = try MockWindexServer()
        server.on("GET /admin/v1/registry") { _ in
            var response = MockWindexServer.Response.json(Self.registryJSON)
            response.headers["ETag"] = "W/\"registry-7\""
            return response
        }
        try await server.start()
        let port = server.port
        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let cache = RegistryCache(client: client, fileURL: file)
        _ = try await cache.load()
        server.stop()

        // Nothing is listening now; the cached copy should still come back.
        let registry = try await cache.load()
        #expect(registry.registryVersion == 7)
        await #expect(cache.wasStale, "the UI needs to know it is showing a stale palette")
        #expect(port > 0)
    }

    /// A fresh process must be able to render before its first request lands.
    @Test("a cached copy is readable without any network call")
    func cachedIsReadableOffline() async throws {
        let file = tempFile()
        defer { try? FileManager.default.removeItem(at: file) }

        let server = try MockWindexServer()
        server.on("GET /admin/v1/registry") { _ in
            var response = MockWindexServer.Response.json(Self.registryJSON)
            response.headers["ETag"] = "W/\"registry-7\""
            return response
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        _ = try await RegistryCache(client: client, fileURL: file).load()

        // A second cache instance, as a relaunch would create.
        let reopened = RegistryCache(client: client, fileURL: file)
        let cached = await reopened.cached()
        #expect(cached?.registryVersion == 7)
        #expect(server.requests.count == 1, "cached() must not hit the network")
    }
}
