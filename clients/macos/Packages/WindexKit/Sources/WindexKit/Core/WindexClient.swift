import Foundation

/// Which of windex's two HTTP contracts a request belongs to.
///
/// They are two contracts with different lifetimes, not one API with a prefix
/// (see the comment above `ops`/`admin` in `api/app.py`): `/v1` is the
/// agent-facing promise — additive-only, open, shrinking toward search + docs +
/// push — while the control plane is separately versioned and will churn.
public enum WindexSurface: Sendable {
    /// `/v1/**` — open on a trusted LAN, no token.
    case agent
    /// `/admin/v1/**` — bearer-gated at the mount.
    ///
    /// Note the prefix. `openapi-admin.json` describes a *mounted* sub-app, so
    /// its paths are mount-relative: the spec's `/v1/health` is really
    /// `/admin/v1/health` on the wire. Getting this wrong 404s every admin call.
    case admin

    var pathPrefix: String {
        switch self {
        case .agent: return ""
        case .admin: return "/admin"
        }
    }

    var requiresToken: Bool { self == .admin }
}

/// Async HTTP client for a windex backend.
///
/// An `actor` because the token is mutable shared state: pairing writes it,
/// every admin request reads it, and a 401 mid-flight may clear it.
public actor WindexClient {

    public struct Configuration: Sendable {
        /// Scheme + host + port only, e.g. `http://192.168.1.237:8100`. Path
        /// components are ignored — surfaces supply their own.
        public var baseURL: URL
        public var timeout: TimeInterval
        /// Longer, separate budget for SSE, which is expected to idle between
        /// events and must not be killed by the request timeout.
        public var streamTimeout: TimeInterval

        public init(baseURL: URL, timeout: TimeInterval = 30,
                    streamTimeout: TimeInterval = 300) {
            self.baseURL = baseURL
            self.timeout = timeout
            self.streamTimeout = streamTimeout
        }
    }

    public private(set) var configuration: Configuration
    /// Internal rather than private so the pairing flow (a separate file) can
    /// save and restore it around a candidate-token attempt. Not public: callers
    /// go through `setToken`/`hasToken` and never read the secret back out.
    var token: String?
    private let session: URLSession
    private let decoder = JSONDecoder()
    private let encoder = JSONEncoder()

    public init(configuration: Configuration, token: String? = nil,
                session: URLSession? = nil) {
        self.configuration = configuration
        self.token = token
        if let session {
            self.session = session
        } else {
            let config = URLSessionConfiguration.ephemeral
            config.timeoutIntervalForRequest = configuration.timeout
            config.waitsForConnectivity = false
            self.session = URLSession(configuration: config)
        }
    }

    public init(baseURL: URL, token: String? = nil) {
        self.init(configuration: Configuration(baseURL: baseURL), token: token)
    }

    // MARK: - Token

    public func setToken(_ token: String?) {
        self.token = token
    }

    public var hasToken: Bool { token?.isEmpty == false }

    // MARK: - Request building

    func url(for path: String, surface: WindexSurface,
             query: [URLQueryItem] = []) throws -> URL {
        guard var components = URLComponents(
            url: configuration.baseURL, resolvingAgainstBaseURL: false
        ) else {
            throw WindexError.invalidURL(configuration.baseURL.absoluteString)
        }
        // `percentEncodedPath`, not `path`: callers hand us paths whose dynamic
        // segments are already escaped (a doc id like `gh:owner/repo`), and
        // assigning to `path` treats the whole string as UNencoded and escapes it
        // again — `%3A` becomes `%253A` and the server 404s on an id that looks
        // correct in every log.
        components.percentEncodedPath = surface.pathPrefix + path
        components.queryItems = query.isEmpty ? nil : query
        guard let url = components.url else {
            throw WindexError.invalidURL(configuration.baseURL.absoluteString + path)
        }
        return url
    }

    private func request(_ method: String, _ path: String, surface: WindexSurface,
                         query: [URLQueryItem], body: Data?) throws -> URLRequest {
        var request = URLRequest(url: try url(for: path, surface: surface, query: query))
        request.httpMethod = method
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if let body {
            request.httpBody = body
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        }
        if surface.requiresToken, let token, !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        return request
    }

    // MARK: - Execution

    /// Perform a request and decode the body.
    public func send<Response: Decodable & Sendable>(
        _ method: String = "GET",
        _ path: String,
        surface: WindexSurface = .agent,
        query: [URLQueryItem] = [],
        body: (any Encodable & Sendable)? = nil,
        as type: Response.Type = Response.self
    ) async throws -> Response {
        let data = try await sendRaw(method, path, surface: surface,
                                     query: query, body: body)
        do {
            return try decoder.decode(Response.self, from: data)
        } catch {
            throw WindexError.decoding(underlying: error)
        }
    }

    /// Perform a request, ignoring the response body.
    @discardableResult
    public func sendRaw(
        _ method: String = "GET",
        _ path: String,
        surface: WindexSurface = .agent,
        query: [URLQueryItem] = [],
        body: (any Encodable & Sendable)? = nil
    ) async throws -> Data {
        var encoded: Data?
        if let body {
            do {
                encoded = try encoder.encode(body)
            } catch {
                throw WindexError.decoding(underlying: error)
            }
        }
        let request = try request(method, path, surface: surface,
                                  query: query, body: encoded)

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw WindexError.transport(underlying: error)
        }

        guard let http = response as? HTTPURLResponse else {
            return data
        }
        guard (200..<300).contains(http.statusCode) else {
            throw Self.error(status: http.statusCode, body: data)
        }
        return data
    }

    /// The result of a conditional GET.
    public enum Conditional<Value: Sendable>: Sendable {
        /// The server returned 304 — the cached copy is still current.
        case notModified
        /// A fresh body, with the validator to send next time.
        case modified(Value, etag: String?)
    }

    /// GET with `If-None-Match`, so an unchanged resource costs a round trip
    /// rather than a payload.
    ///
    /// Only worth it for something big and stable that a client keeps a copy of.
    /// That is `/admin/v1/registry`: the graph editor renders its whole palette
    /// from it, so it is fetched on launch and revalidated after — the server
    /// sends `Cache-Control: no-cache`, meaning "always check", not "don't
    /// store".
    public func sendConditional<Response: Decodable & Sendable>(
        _ path: String,
        surface: WindexSurface = .admin,
        etag: String?,
        as type: Response.Type = Response.self
    ) async throws -> Conditional<Response> {
        var request = try request("GET", path, surface: surface, query: [], body: nil)
        if let etag, !etag.isEmpty {
            request.setValue(etag, forHTTPHeaderField: "If-None-Match")
        }

        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw WindexError.transport(underlying: error)
        }

        guard let http = response as? HTTPURLResponse else {
            throw WindexError.decoding(
                underlying: WindexError.http(status: 0, message: "not an HTTP response"))
        }
        if http.statusCode == 304 {
            return .notModified
        }
        guard (200..<300).contains(http.statusCode) else {
            throw Self.error(status: http.statusCode, body: data)
        }
        do {
            let value = try decoder.decode(Response.self, from: data)
            return .modified(value, etag: http.value(forHTTPHeaderField: "ETag"))
        } catch {
            throw WindexError.decoding(underlying: error)
        }
    }

    /// Map a non-2xx response onto a typed error, pulling FastAPI's `detail` out
    /// of the body so the server's own wording reaches the operator.
    static func error(status: Int, body: Data) -> WindexError {
        let root = try? JSONDecoder().decode(JSONValue.self, from: body)
        let detail = root?.objectValue?["detail"]

        // 422 carries a list of per-field failures rather than a string.
        if status == 422, let raw = detail,
           let failures = try? JSONDecoder().decode(
               [ValidationFailure].self, from: JSONEncoder().encode(raw)) {
            return .validation(failures: failures,
                               message: "the server rejected these values")
        }

        let message = detail?.stringValue
            ?? String(data: body, encoding: .utf8).flatMap { $0.isEmpty ? nil : $0 }
            ?? "HTTP \(status)"

        switch status {
        case 401:
            return .unauthorized(message: message)
        case 404:
            return .notFound(message: message)
        case 503:
            // The admin gate returns 503 with fix-it instructions when bound
            // off-loopback without a token — distinct from a transient outage,
            // and the operator needs the text.
            return .adminDisabled(message: message)
        default:
            return .http(status: status, message: message)
        }
    }

    // MARK: - Streaming (SSE)

    /// Open a Server-Sent Events stream. The caller consumes events until it
    /// stops iterating or the server closes.
    public func events(_ path: String, surface: WindexSurface = .agent,
                       query: [URLQueryItem] = []) throws -> AsyncThrowingStream<SSEEvent, any Error> {
        var request = URLRequest(url: try url(for: path, surface: surface, query: query))
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        // A cached SSE response would replay a finished stream instead of opening
        // a live one.
        request.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
        request.timeoutInterval = configuration.streamTimeout
        if surface.requiresToken, let token, !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        return SSEClient.stream(request: request, session: session)
    }
}
