import Foundation

// `Health` is generated and `WhoAmI` is intentionally open — see Wire.swift.
// hangs `isOK`/`isWindex`/`needsToken` off them. What lives here is the pairing
// FLOW, which is behaviour rather than wire shape.

/// The outcome of a pairing attempt.
public enum PairingResult: Sendable, Hashable {
    /// Token accepted (or not needed). Safe to persist.
    case paired(Health, WhoAmI)
    /// A windex backend is there, it wants a token, and none was supplied.
    case tokenRequired(Health)
    /// A backend answered but isn't windex.
    case notWindex(service: String)
}

extension WindexClient {

    /// Liveness + capability probe. No token required, by design.
    public func health() async throws -> Health {
        try await send("GET", "/v1/health", surface: .admin, as: Health.self)
    }

    /// Validate the current token. Throws `.unauthorized` if it isn't accepted.
    public func whoami() async throws -> WhoAmI {
        try await send("GET", "/v1/whoami", surface: .admin, as: WhoAmI.self)
    }

    /// The full pairing handshake: probe, then prove the token works before the
    /// caller saves it.
    ///
    /// The order matters and is the whole point of having two endpoints. `health`
    /// is open, so it answers "is there a windex here, and does it want a token"
    /// without one. Only then is `whoami` called with the candidate token — a 200
    /// there is the sole proof the token is accepted. Persisting a token that was
    /// merely *typed* moves the failure to the first write, which is exactly what
    /// the gated echo exists to prevent.
    ///
    /// The token is left set on the client on success and restored on failure, so
    /// a rejected attempt can't leave a bad token behind.
    public func pair(with candidateToken: String?) async throws -> PairingResult {
        let health = try await health()
        guard health.isWindex else {
            return .notWindex(service: health.service)
        }
        guard health.isSupportedEpoch else {
            throw WindexError.unsupportedContractEpoch(
                received: health.contractEpoch,
                supported: 2
            )
        }

        let trimmed = candidateToken?.trimmingCharacters(in: .whitespacesAndNewlines)
        if health.needsToken && (trimmed?.isEmpty ?? true) {
            return .tokenRequired(health)
        }

        let previous = token
        setToken(trimmed)
        do {
            let who = try await whoami()
            return .paired(health, who)
        } catch {
            setToken(previous)
            throw error
        }
    }
}
