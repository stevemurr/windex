import Foundation
import Testing
@testable import WindexKit

/// SSE framing has a handful of rules that are easy to get subtly wrong and that
/// fail only under real chunking — which is why the mock server writes these
/// streams across awkward buffer boundaries rather than one write per event.
@Suite("SSE")
struct SSETests {

    @Test("events arriving across arbitrary chunk boundaries reassemble")
    func reassemblesAcrossChunks() async throws {
        let server = try MockWindexServer()
        server.on("GET /v1/events") { _ in .sse(Fixtures.sseChunks) }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        var received: [SSEEvent] = []
        for try await event in try await client.events("/v1/events") {
            received.append(event)
        }

        // The `: keep-alive` comment must NOT have produced an event.
        #expect(received.count == 4)
        #expect(received.map(\.event) == ["tick", "tick", "note", "tick"])

        // An event split mid-field name ("da" + "ta: ...") still parses.
        #expect(received[1].data == "{\"n\":2}")

        // Repeated data: lines join with a newline, per spec.
        #expect(received[2].data == "line one\nline two")

        #expect(received[3].id == "7")
        #expect(try received[3].decode([String: Int].self) == ["n": 3])
    }

    @Test("a stream on the admin surface carries the token and the prefix")
    func adminStreamIsAuthenticated() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/events/stream") { _ in
            .sse(["event: done\ndata: {}\n\n"])
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "s3cret")
        var count = 0
        for try await _ in try await client.events("/v1/events/stream",
                                                   surface: .admin,
                                                   lastEventID: "41") {
            count += 1
        }

        #expect(count == 1)
        let sent = try #require(server.lastRequest)
        #expect(sent.path == "/admin/v1/events/stream")
        #expect(sent.header("Authorization") == "Bearer s3cret")
        #expect(sent.header("Last-Event-ID") == "41")
        #expect(sent.header("Accept") == "text/event-stream")
    }

    /// A stream that 401s must surface as `.unauthorized`, not as an empty
    /// stream — an empty stream reads as "nothing is happening", which is a
    /// materially wrong thing to show an operator watching a crawl.
    @Test("an unauthorized stream throws rather than ending quietly")
    func unauthorizedStreamThrows() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/events/stream") { _ in
            .detail("missing or invalid admin token", status: 401)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "bad")
        var caught: WindexError?
        do {
            for try await _ in try await client.events("/v1/events/stream", surface: .admin) {}
        } catch let error as WindexError {
            caught = error
        }

        guard case .unauthorized = caught else {
            Issue.record("expected .unauthorized, got \(String(describing: caught))")
            return
        }
    }

    /// Abandoning the loop must cancel the underlying request rather than leave
    /// it running — a Settings screen the operator navigates away from should not
    /// hold a connection open.
    @Test("breaking out of the loop terminates the stream")
    func earlyExitCancels() async throws {
        let many = (0..<200).map { "event: tick\ndata: {\"n\":\($0)}\n\n" }
        let server = try MockWindexServer()
        server.on("GET /v1/events") { _ in .sse(many) }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        var seen = 0
        for try await _ in try await client.events("/v1/events") {
            seen += 1
            if seen == 3 { break }
        }
        #expect(seen == 3)
    }

    // MARK: - Parser units

    @Test("a comment-only line yields no event")
    func commentsAreIgnored() {
        var parser = SSEParser()
        #expect(parser.consume(line: ": keep-alive") == nil)
        #expect(parser.consume(line: "") == nil)
    }

    @Test("exactly one leading space after the colon is stripped")
    func stripsOneLeadingSpace() {
        var parser = SSEParser()
        _ = parser.consume(line: "data:  two spaces")
        let event = parser.consume(line: "")
        #expect(event?.data == " two spaces")
    }

    @Test("a field with no colon is a valid field with an empty value")
    func bareFieldName() {
        var parser = SSEParser()
        _ = parser.consume(line: "data")
        let event = parser.consume(line: "")
        #expect(event?.data == "")
    }

    @Test("an event with no name defaults to message")
    func defaultEventName() {
        var parser = SSEParser()
        _ = parser.consume(line: "data: hello")
        #expect(parser.consume(line: "")?.event == "message")
    }

    @Test("CRLF line endings are tolerated")
    func handlesCRLF() {
        var parser = SSEParser()
        _ = parser.consume(line: "event: tick\r")
        _ = parser.consume(line: "data: 1\r")
        let event = parser.consume(line: "\r")
        #expect(event?.event == "tick")
        #expect(event?.data == "1")
    }

    /// `id` is connection-level state — a reconnect resumes from the last one —
    /// so it persists across events, unlike `event`/`data`.
    @Test("id persists across events but the event name does not")
    func idPersistsEventNameDoesNot() {
        var parser = SSEParser()
        _ = parser.consume(line: "id: 42")
        _ = parser.consume(line: "event: tick")
        _ = parser.consume(line: "data: 1")
        let first = parser.consume(line: "")

        _ = parser.consume(line: "data: 2")
        let second = parser.consume(line: "")

        #expect(first?.id == "42")
        #expect(first?.event == "tick")
        #expect(second?.id == "42", "id is connection state")
        #expect(second?.event == "message", "event name resets per event")
    }
}
