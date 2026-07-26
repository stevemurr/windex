import Foundation
import Testing
@testable import WindexKit

/// Pairing is `health` (open) then `whoami` (gated) — the order is the contract,
/// not an implementation detail, so these tests assert on it directly.
@Suite("Pairing")
struct PairingTests {

    @Test("health answers without a token, and reports whether one is needed")
    func healthIsOpen() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/health") { _ in .json(Fixtures.health(authRequired: true)) }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let health = try await client.health()

        #expect(health.isOK)
        #expect(health.isWindex)
        #expect(health.needsToken)
        #expect(health.version == "0.1.0")
        // The open route must not have been sent credentials it doesn't need.
        #expect(server.lastRequest?.header("Authorization") == nil)
    }

    /// The whole reason `/admin/v1/health` exists as the single unauthenticated
    /// admin route: the app can discover a backend before it has anything to
    /// authenticate with.
    @Test("a token-requiring backend is reported, not guessed at")
    func tokenRequired() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/health") { _ in .json(Fixtures.health(authRequired: true)) }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let result = try await client.pair(with: nil)

        guard case .tokenRequired = result else {
            Issue.record("expected .tokenRequired, got \(result)")
            return
        }
        // whoami must NOT have been attempted without a token.
        #expect(server.requests.count == 1)
    }

    @Test("a valid token is proven against whoami before it can be saved")
    func validTokenIsProven() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/health") { _ in .json(Fixtures.health(authRequired: true)) }
        server.on("GET /admin/v1/whoami") { request in
            request.header("Authorization") == "Bearer s3cret"
                ? .json(Fixtures.whoami)
                : .detail("missing or invalid admin token", status: 401)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let result = try await client.pair(with: "s3cret")

        guard case .paired(_, let who) = result else {
            Issue.record("expected .paired, got \(result)")
            return
        }
        #expect(who["ok"] == .bool(true))
        #expect(who["scopes"]?.stringArrayValue == ["admin"])
        let hasToken = await client.hasToken
        #expect(hasToken)
        #expect(server.requests.map(\.path)
            == ["/admin/v1/health", "/admin/v1/whoami"])
    }

    @Test("pairing refuses any contract epoch other than two")
    func rejectsWrongEpoch() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/health") { _ in
            .json(Fixtures.health(authRequired: false)
                .replacingOccurrences(of: "\"contract_epoch\":2",
                                      with: "\"contract_epoch\":1"))
        }
        try await server.start()
        defer { server.stop() }
        let client = WindexClient(baseURL: server.baseURL)
        await #expect(throws: WindexError.self) {
            _ = try await client.pair(with: nil)
        }
        #expect(server.requests.count == 1)
    }

    /// A rejected token must not be left on the client, or the next admin call
    /// would fail with a stale credential instead of prompting to re-pair.
    @Test("a rejected token is not retained")
    func rejectedTokenIsNotRetained() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/health") { _ in .json(Fixtures.health(authRequired: true)) }
        server.on("GET /admin/v1/whoami") { _ in
            .detail("missing or invalid admin token", status: 401)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)

        await #expect(throws: WindexError.self) {
            _ = try await client.pair(with: "wrong")
        }
        let hasToken = await client.hasToken
        #expect(!hasToken)
    }

    @Test("401 maps to .unauthorized and asks for re-pairing")
    func unauthorizedIsTyped() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/whoami") { _ in
            .detail("missing or invalid admin token", status: 401)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "bad")
        do {
            _ = try await client.whoami()
            Issue.record("expected a throw")
        } catch let error as WindexError {
            guard case .unauthorized(let message) = error else {
                Issue.record("expected .unauthorized, got \(error)")
                return
            }
            #expect(message == "missing or invalid admin token")
            #expect(error.needsPairing)
        }
    }

    /// Binding off-loopback with no token disables the admin surface. The server
    /// answers 503 with fix-it instructions, and that text is the useful part —
    /// it must reach the operator rather than being flattened to "server error".
    @Test("503 carries the server's fix-it message")
    func adminDisabledKeepsServerWording() async throws {
        let fixIt = "admin API disabled: set WINDEX_WRITE_TOKEN, or bind "
            + "WINDEX_SERVE_HOST=127.0.0.1."
        let server = try MockWindexServer()
        server.on("GET /admin/v1/whoami") { _ in .detail(fixIt, status: 503) }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        do {
            _ = try await client.whoami()
            Issue.record("expected a throw")
        } catch let error as WindexError {
            guard case .adminDisabled(let message) = error else {
                Issue.record("expected .adminDisabled, got \(error)")
                return
            }
            #expect(message == fixIt)
            #expect(!error.needsPairing)   // a token won't fix this
        }
    }

    /// Typing a LAN address by hand can easily land on some other service
    /// listening on :8100. Pairing should say so rather than fail obscurely later.
    @Test("a non-windex backend is identified")
    func notWindex() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/health") { _ in
            .json(Fixtures.health(authRequired: false, service: "grafana"))
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let result = try await client.pair(with: nil)

        guard case .notWindex(let service) = result else {
            Issue.record("expected .notWindex, got \(result)")
            return
        }
        #expect(service == "grafana")
    }

    /// Loopback with no token set is the trusted local-dev path; pairing should
    /// succeed with no credential at all.
    @Test("an open backend pairs with no token")
    func openBackendPairs() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/health") { _ in .json(Fixtures.health(authRequired: false)) }
        server.on("GET /admin/v1/whoami") { _ in
            .json("""
                {"ok":true,"scopes":["admin"],"auth_required":false}
                """)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL)
        let result = try await client.pair(with: nil)

        guard case .paired = result else {
            Issue.record("expected .paired, got \(result)")
            return
        }
    }

    /// An unreachable host must surface as `.transport`, not as a decode failure
    /// — on this LAN it is also how a macOS Local Network TCC denial presents.
    @Test("an unreachable backend is a transport error")
    func unreachableIsTransport() async throws {
        // Port 1 is reserved and nothing listens there.
        let client = WindexClient(baseURL: URL(string: "http://127.0.0.1:1")!)
        do {
            _ = try await client.health()
            Issue.record("expected a throw")
        } catch let error as WindexError {
            guard case .transport = error else {
                Issue.record("expected .transport, got \(error)")
                return
            }
        }
    }
}
