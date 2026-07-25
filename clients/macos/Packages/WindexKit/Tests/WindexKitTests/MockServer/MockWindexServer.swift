import Foundation
import Network

/// A real HTTP server on localhost, used as the backend for WindexKit's
/// integration tests.
///
/// **Why a socket and not a `URLProtocol` stub.** The point of these tests is
/// that swapping in the real backend changes exactly one thing — the base URL —
/// and everything else keeps working. A `URLProtocol` stub short-circuits
/// `URLSession` above the network stack, so it would happily pass while the
/// things most likely to actually break went untested: query-string encoding on
/// the wire, whether the `Authorization` header is really attached to `/admin`
/// requests and really absent from `/v1` ones, status-code mapping, chunked SSE
/// framing arriving in arbitrary buffer splits, and the `/admin` mount prefix.
/// Those are the bugs this suite exists to catch, so the transport has to be real.
///
/// Built on `Network.framework` rather than a Swift HTTP server package to keep
/// WindexKit dependency-free — see the note in Package.swift.
final class MockWindexServer: @unchecked Sendable {

    /// A recorded request, for assertions about what the client actually sent.
    struct Recorded: Sendable, Hashable {
        let method: String
        let path: String
        let query: [String: String]
        let headers: [String: String]
        let body: String

        /// Header lookup is case-insensitive, as HTTP requires.
        func header(_ name: String) -> String? {
            headers.first { $0.key.lowercased() == name.lowercased() }?.value
        }
    }

    /// What a handler returns.
    struct Response: Sendable {
        var status: Int = 200
        var headers: [String: String] = ["Content-Type": "application/json"]
        var body: Data = Data()
        /// When set, the response is streamed as SSE: each chunk is written with
        /// a small delay so the client sees genuinely incremental delivery.
        var sseChunks: [String]?

        static func json(_ raw: String, status: Int = 200) -> Response {
            Response(status: status, body: Data(raw.utf8))
        }

        static func json(_ value: JSONEncodable, status: Int = 200) -> Response {
            Response(status: status, body: value.encoded())
        }

        /// FastAPI's error shape, so the client's `detail` extraction is exercised.
        static func detail(_ message: String, status: Int) -> Response {
            let escaped = message.replacingOccurrences(of: "\\", with: "\\\\")
                .replacingOccurrences(of: "\"", with: "\\\"")
            return .json("{\"detail\":\"\(escaped)\"}", status: status)
        }

        static func sse(_ chunks: [String]) -> Response {
            Response(status: 200,
                     headers: ["Content-Type": "text/event-stream",
                               "Cache-Control": "no-cache"],
                     sseChunks: chunks)
        }
    }

    typealias Handler = @Sendable (Recorded) -> Response

    private let listener: NWListener
    private let queue = DispatchQueue(label: "mock-windex-server")
    private let lock = NSLock()
    private var routes: [String: Handler] = [:]
    private var _requests: [Recorded] = []
    private var connections: [NWConnection] = []

    /// Every request the server has received, in order.
    var requests: [Recorded] {
        lock.lock(); defer { lock.unlock() }
        return _requests
    }

    var lastRequest: Recorded? { requests.last }

    private(set) var port: UInt16 = 0

    /// The address to hand a `WindexClient`. Loopback, plain HTTP — same shape as
    /// the real LAN backend, which is why the app needs `NSAllowsLocalNetworking`
    /// rather than a blanket ATS exception.
    var baseURL: URL { URL(string: "http://127.0.0.1:\(port)")! }

    init() throws {
        let params = NWParameters.tcp
        params.allowLocalEndpointReuse = true
        listener = try NWListener(using: params, on: .any)
    }

    // MARK: - Routing

    /// Register a handler. `route` is `"<METHOD> <path>"`, e.g.
    /// `"GET /admin/v1/health"` — the FULL wire path, including the `/admin`
    /// mount prefix, so a client that forgets the prefix gets a 404 here exactly
    /// as it would from the real server.
    func on(_ route: String, _ handler: @escaping Handler) {
        lock.lock(); defer { lock.unlock() }
        routes[route] = handler
    }

    /// Register a static JSON body.
    func on(_ route: String, json: String, status: Int = 200) {
        on(route) { _ in .json(json, status: status) }
    }

    // MARK: - Lifecycle

    func start() async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, any Error>) in
            let resumed = OSAllocatedUnfairLockBox(false)
            listener.stateUpdateHandler = { [weak self] state in
                switch state {
                case .ready:
                    self?.port = self?.listener.port?.rawValue ?? 0
                    if resumed.setTrueIfFalse() { cont.resume() }
                case .failed(let error):
                    if resumed.setTrueIfFalse() { cont.resume(throwing: error) }
                default:
                    break
                }
            }
            listener.newConnectionHandler = { [weak self] connection in
                self?.accept(connection)
            }
            listener.start(queue: queue)
        }
    }

    func stop() {
        listener.cancel()
        lock.lock()
        let open = connections
        connections = []
        lock.unlock()
        open.forEach { $0.cancel() }
    }

    // MARK: - Connection handling

    private func accept(_ connection: NWConnection) {
        lock.lock()
        connections.append(connection)
        lock.unlock()
        connection.start(queue: queue)
        receive(on: connection, buffer: Data())
    }

    private func receive(on connection: NWConnection, buffer: Data) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 65536) {
            [weak self] chunk, _, isComplete, error in
            guard let self else { return }
            if error != nil {
                connection.cancel()
                return
            }
            var buffer = buffer
            if let chunk { buffer.append(chunk) }

            // A request is complete once headers have landed and the body has
            // reached Content-Length. Anything less means read again — the
            // client's PATCH body can arrive in a separate segment from its
            // headers, and treating a partial read as the whole request is the
            // classic way a hand-rolled test server becomes flaky under load.
            if let request = Self.parse(buffer) {
                self.handle(request, on: connection)
            } else if isComplete {
                connection.cancel()
            } else {
                self.receive(on: connection, buffer: buffer)
            }
        }
    }

    private func handle(_ request: Recorded, on connection: NWConnection) {
        lock.lock()
        _requests.append(request)
        let handler = routes["\(request.method) \(request.path)"]
        lock.unlock()

        let response = handler?(request)
            ?? .detail("Not Found", status: 404)

        if let chunks = response.sseChunks {
            writeSSE(response, chunks: chunks, on: connection)
        } else {
            connection.send(content: Self.serialize(response),
                            completion: .contentProcessed { _ in connection.cancel() })
        }
    }

    /// Stream an SSE response chunk by chunk, so the client's parser has to cope
    /// with events arriving across separate reads rather than one tidy buffer.
    private func writeSSE(_ response: Response, chunks: [String], on connection: NWConnection) {
        var head = "HTTP/1.1 200 OK\r\n"
        for (key, value) in response.headers {
            head += "\(key): \(value)\r\n"
        }
        head += "Connection: close\r\n\r\n"

        connection.send(content: Data(head.utf8), completion: .contentProcessed { _ in
            func writeNext(_ index: Int) {
                guard index < chunks.count else {
                    connection.cancel()      // closing ends the stream
                    return
                }
                connection.send(
                    content: Data(chunks[index].utf8),
                    completion: .contentProcessed { _ in
                        self.queue.asyncAfter(deadline: .now() + 0.01) {
                            writeNext(index + 1)
                        }
                    })
            }
            writeNext(0)
        })
    }

    // MARK: - HTTP wire format

    private static func parse(_ buffer: Data) -> Recorded? {
        guard let headerEnd = buffer.range(of: Data("\r\n\r\n".utf8)) else { return nil }
        guard let head = String(data: buffer[..<headerEnd.lowerBound], encoding: .utf8)
        else { return nil }

        var lines = head.components(separatedBy: "\r\n")
        guard !lines.isEmpty else { return nil }
        let requestLine = lines.removeFirst().split(separator: " ", maxSplits: 2)
        guard requestLine.count >= 2 else { return nil }

        var headers: [String: String] = [:]
        for line in lines {
            guard let colon = line.firstIndex(of: ":") else { continue }
            let name = String(line[line.startIndex..<colon])
            let value = String(line[line.index(after: colon)...])
                .trimmingCharacters(in: .whitespaces)
            headers[name] = value
        }

        let bodyStart = headerEnd.upperBound
        let available = buffer.count - bodyStart
        let expected = headers.first { $0.key.lowercased() == "content-length" }
            .flatMap { Int($0.value) } ?? 0
        guard available >= expected else { return nil }   // body still arriving

        let body = String(
            data: buffer[bodyStart..<(bodyStart + expected)], encoding: .utf8) ?? ""

        let target = String(requestLine[1])
        let components = URLComponents(string: "http://x" + target)
        var query: [String: String] = [:]
        for item in components?.queryItems ?? [] {
            query[item.name] = item.value ?? ""
        }

        return Recorded(
            method: String(requestLine[0]),
            path: components?.path ?? target,
            query: query,
            headers: headers,
            body: body
        )
    }

    private static func serialize(_ response: Response) -> Data {
        var head = "HTTP/1.1 \(response.status) \(reason(response.status))\r\n"
        for (key, value) in response.headers {
            head += "\(key): \(value)\r\n"
        }
        head += "Content-Length: \(response.body.count)\r\n"
        head += "Connection: close\r\n\r\n"
        return Data(head.utf8) + response.body
    }

    private static func reason(_ status: Int) -> String {
        switch status {
        case 200: return "OK"
        case 201: return "Created"
        case 204: return "No Content"
        case 400: return "Bad Request"
        case 401: return "Unauthorized"
        case 404: return "Not Found"
        case 422: return "Unprocessable Entity"
        case 500: return "Internal Server Error"
        case 503: return "Service Unavailable"
        default: return "Status"
        }
    }
}

// MARK: - Helpers

/// Minimal box so the listener's state handler can resume its continuation
/// exactly once — `.ready` can be delivered more than once.
private final class OSAllocatedUnfairLockBox: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Bool
    init(_ value: Bool) { self.value = value }

    /// Returns true only for the first caller.
    func setTrueIfFalse() -> Bool {
        lock.lock(); defer { lock.unlock() }
        if value { return false }
        value = true
        return true
    }
}

/// Anything the mock can serialize as a JSON body.
protocol JSONEncodable: Sendable {
    func encoded() -> Data
}

extension String: JSONEncodable {
    func encoded() -> Data { Data(utf8) }
}
