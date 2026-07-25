import Foundation

/// One entry from FastAPI's 422 body (`ValidationError` in the OpenAPI schema).
public struct ValidationFailure: Sendable, Hashable, Codable {
    /// Path to the offending input, e.g. `["query", "limit"]`.
    public let loc: [String]
    public let msg: String
    public let type: String

    private enum CodingKeys: String, CodingKey { case loc, msg, type }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        // `loc` members are strings or ints (a list index); render both as text.
        loc = (try c.decode([JSONValue].self, forKey: .loc)).map {
            $0.stringValue ?? $0.intValue.map(String.init) ?? ""
        }
        msg = try c.decode(String.self, forKey: .msg)
        type = try c.decode(String.self, forKey: .type)
    }

    /// The field this failure is about, ignoring the leading location kind
    /// (`body`, `query`, …) FastAPI prefixes.
    public var field: String? { loc.count > 1 ? loc.last : loc.first }
}

public enum WindexError: Error, Sendable {
    /// The host didn't answer at all — wrong address, service down, or (a real
    /// case on this LAN) macOS denied Local Network access to the process.
    case transport(underlying: any Error)

    /// Missing or invalid token on an `/admin` route. The client should send the
    /// operator back to pairing rather than retrying.
    case unauthorized(message: String)

    /// The admin surface is bound off-loopback with no token set, so the server
    /// disabled it. `message` carries the server's fix-it instructions verbatim
    /// — it tells the operator exactly what to set, so don't replace it.
    case adminDisabled(message: String)

    case notFound(message: String)

    /// 422. Carries the per-field failures so a form can attach them to controls.
    case validation(failures: [ValidationFailure], message: String)

    /// Any other non-2xx.
    case http(status: Int, message: String)

    /// The body didn't match the expected shape.
    case decoding(underlying: any Error)

    /// The base URL couldn't be formed into a request URL.
    case invalidURL(String)
}

extension WindexError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .transport(let underlying):
            return underlying.localizedDescription
        case .unauthorized(let message):
            return message
        case .adminDisabled(let message):
            return message
        case .notFound(let message):
            return message
        case .validation(let failures, let message):
            guard !failures.isEmpty else { return message }
            return failures
                .map { f in f.field.map { "\($0): \(f.msg)" } ?? f.msg }
                .joined(separator: "\n")
        case .http(let status, let message):
            return message.isEmpty ? "HTTP \(status)" : message
        case .decoding(let underlying):
            return "unexpected response from server: \(underlying)"
        case .invalidURL(let raw):
            return "not a usable server address: \(raw)"
        }
    }

    /// Whether re-pairing (a new token) could plausibly fix this.
    public var needsPairing: Bool {
        if case .unauthorized = self { return true }
        return false
    }
}
