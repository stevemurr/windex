import Foundation
import Observation
import WindexKit

enum SidebarDestination: String, CaseIterable, Hashable, Identifiable, Sendable {
    case overview
    case sources
    case pipelines
    case runs
    case logs
    case search
    case settings

    var id: Self { self }

    var title: String {
        switch self {
        case .overview: "Overview"
        case .sources: "Sources"
        case .pipelines: "Pipelines"
        case .runs: "Runs"
        case .logs: "Logs"
        case .search: "Search"
        case .settings: "Settings"
        }
    }

    var systemImage: String {
        switch self {
        case .overview: "rectangle.3.group"
        case .sources: "tray.full"
        case .pipelines: "point.3.connected.trianglepath.dotted"
        case .runs: "waveform.path.ecg"
        case .logs: "text.alignleft"
        case .search: "magnifyingglass"
        case .settings: "slider.horizontal.3"
        }
    }
}

struct ConnectionProfile: Equatable, Hashable, Sendable {
    let baseURL: URL

    init(_ input: String) throws {
        var value = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !value.isEmpty else {
            throw ConnectionProfileError.empty
        }
        if !value.contains("://") {
            value = "http://" + value
        }
        guard var components = URLComponents(string: value),
              let scheme = components.scheme?.lowercased(),
              scheme == "http" || scheme == "https",
              components.host?.isEmpty == false else {
            throw ConnectionProfileError.invalid
        }
        components.scheme = scheme
        components.path = ""
        components.query = nil
        components.fragment = nil
        guard let url = components.url else {
            throw ConnectionProfileError.invalid
        }
        baseURL = url
    }

    var displayAddress: String {
        baseURL.absoluteString.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    }

    var credentialAccount: String { displayAddress }
}

enum ConnectionProfileError: LocalizedError, Equatable {
    case empty
    case invalid

    var errorDescription: String? {
        switch self {
        case .empty:
            "Enter the address of your windex backend."
        case .invalid:
            "Use an HTTP or HTTPS address with a host, such as spark.local:8100."
        }
    }
}

protocol BackendAddressStoring: Sendable {
    func load() -> String?
    func save(_ address: String)
    func remove()
}

final class UserDefaultsBackendAddressStore: BackendAddressStoring, @unchecked Sendable {
    private let defaults: UserDefaults
    private let key: String

    init(defaults: UserDefaults = .standard,
         key: String = "windex.backend-address") {
        self.defaults = defaults
        self.key = key
    }

    func load() -> String? { defaults.string(forKey: key) }
    func save(_ address: String) { defaults.set(address, forKey: key) }
    func remove() { defaults.removeObject(forKey: key) }
}

struct PairingEvidence: Equatable, Sendable {
    let version: String?
    let uptimeSeconds: Int
    let authRequired: Bool
    let scopes: [String]
    let contractEpoch: Int?

    init(
        version: String?,
        uptimeSeconds: Int,
        authRequired: Bool,
        scopes: [String],
        contractEpoch: Int? = nil
    ) {
        self.version = version
        self.uptimeSeconds = uptimeSeconds
        self.authRequired = authRequired
        self.scopes = scopes
        self.contractEpoch = contractEpoch
    }
}

enum PairingOutcome: Equatable, Sendable {
    case paired(PairingEvidence)
    case tokenRequired
    case notWindex(service: String)
    case unauthorized
    case adminDisabled(String)
}

struct ConnectedBackend: Equatable, Sendable {
    let profile: ConnectionProfile
    let evidence: PairingEvidence
    let hasStoredToken: Bool
}

enum ConnectionFailure: Equatable, Sendable {
    case invalidAddress(String)
    case unreachable(address: String)
    case unauthorized
    case adminDisabled(String)
    case notWindex(service: String)
    case incompatibleContract(String)
    case credential(String)
    case unexpected(String)

    var title: String {
        switch self {
        case .invalidAddress:
            "That address cannot be used."
        case .unreachable(let address):
            "Can’t reach windex at \(address)."
        case .unauthorized:
            "This token was rejected."
        case .adminDisabled:
            "Admin access is disabled."
        case .notWindex:
            "That service is not windex."
        case .incompatibleContract:
            "This backend contract is incompatible."
        case .credential:
            "The credential could not be stored."
        case .unexpected:
            "The backend returned an unexpected response."
        }
    }

    var guidance: String {
        switch self {
        case .invalidAddress(let message), .credential(let message),
             .adminDisabled(let message), .incompatibleContract(let message),
             .unexpected(let message):
            message
        case .unreachable:
            "The backend may be down, or this Mac may be off the network."
        case .unauthorized:
            "Pair again to continue."
        case .notWindex(let service):
            "The address answered as “\(service)”. Check the host and port."
        }
    }
}

@MainActor
@Observable
final class AppModel {
    enum ConnectionState: Equatable {
        case unconfigured
        case connecting(ConnectionProfile)
        case tokenRequired(ConnectionProfile)
        case ready(ConnectedBackend)
        case failed(ConnectionProfile?, ConnectionFailure)
    }

    typealias PairingOperation =
        @Sendable (WindexClient, String?) async throws -> PairingOutcome

    var selection: SidebarDestination = .overview
    var connectionState: ConnectionState = .unconfigured
    var backendAddress = ""
    private(set) var client: WindexClient?
    private(set) var session: BackendSession?

    private let tokenStore: any TokenStoring
    private let addressStore: any BackendAddressStoring
    private let pairClient: PairingOperation

    init(
        tokenStore: any TokenStoring = KeychainTokenStore(),
        addressStore: any BackendAddressStoring = UserDefaultsBackendAddressStore(),
        pairClient: @escaping PairingOperation = { client, token in
            do {
                switch try await client.pair(with: token) {
                case .paired(let health, let identity):
                    let scopes = identity["scopes"]?.stringArrayValue ?? []
                    return .paired(
                        PairingEvidence(
                            version: health.version,
                            uptimeSeconds: Int(health.uptimeS),
                            authRequired: health.needsToken,
                            scopes: scopes,
                            contractEpoch: health.contractEpoch))
                case .tokenRequired:
                    return .tokenRequired
                case .notWindex(let service):
                    return .notWindex(service: service)
                }
            } catch let error as WindexError {
                switch error {
                case .unauthorized:
                    return .unauthorized
                case .adminDisabled(let message):
                    return .adminDisabled(message)
                default:
                    throw error
                }
            }
        }
    ) {
        self.tokenStore = tokenStore
        self.addressStore = addressStore
        self.pairClient = pairClient
    }

    var connectedBackend: ConnectedBackend? {
        guard case .ready(let backend) = connectionState else { return nil }
        return backend
    }

    var currentProfile: ConnectionProfile? {
        switch connectionState {
        case .connecting(let profile), .tokenRequired(let profile):
            profile
        case .ready(let backend):
            backend.profile
        case .failed(let profile, _):
            profile
        case .unconfigured:
            nil
        }
    }

    func restore() async {
        guard case .unconfigured = connectionState,
              let saved = addressStore.load(), !saved.isEmpty else {
            return
        }
        backendAddress = saved
        await connect(saved, useStoredToken: true)
    }

    func connect(
        _ input: String,
        candidateToken: String? = nil,
        useStoredToken: Bool = false
    ) async {
        let profile: ConnectionProfile
        do {
            profile = try ConnectionProfile(input)
        } catch {
            disconnectSession()
            client = nil
            connectionState = .failed(
                nil,
                .invalidAddress(
                    (error as? LocalizedError)?.errorDescription
                        ?? "Enter the backend’s HTTP or HTTPS address."))
            return
        }

        backendAddress = profile.displayAddress
        connectionState = .connecting(profile)

        let token: String?
        do {
            token = useStoredToken
                ? try tokenStore.loadToken(for: profile)
                : candidateToken?.trimmingCharacters(in: .whitespacesAndNewlines)
        } catch {
            disconnectSession()
            client = nil
            connectionState = .failed(
                profile,
                .credential(
                    (error as? LocalizedError)?.errorDescription
                        ?? "Keychain access failed."))
            return
        }

        let candidate = token?.isEmpty == false ? token : nil
        let connectingClient = WindexClient(baseURL: profile.baseURL, token: candidate)
        client = connectingClient

        do {
            switch try await pairClient(connectingClient, candidate) {
            case .paired(let evidence):
                try persistVerifiedToken(candidate, for: profile)
                addressStore.save(profile.displayAddress)
                let backend = ConnectedBackend(
                    profile: profile,
                    evidence: evidence,
                    hasStoredToken: candidate != nil)
                disconnectSession()
                client = connectingClient
                session = BackendSession(client: connectingClient, backend: backend)
                connectionState = .ready(backend)

            case .tokenRequired:
                if useStoredToken {
                    try? tokenStore.deleteToken(for: profile)
                }
                addressStore.save(profile.displayAddress)
                disconnectSession()
                client = nil
                connectionState = .tokenRequired(profile)

            case .notWindex(let service):
                disconnectSession()
                client = nil
                connectionState = .failed(profile, .notWindex(service: service))

            case .unauthorized:
                if useStoredToken {
                    try? tokenStore.deleteToken(for: profile)
                }
                disconnectSession()
                client = nil
                connectionState = .failed(profile, .unauthorized)

            case .adminDisabled(let message):
                disconnectSession()
                client = nil
                connectionState = .failed(profile, .adminDisabled(message))
            }
        } catch {
            disconnectSession()
            client = nil
            if useStoredToken, let windexError = error as? WindexError,
               case .unauthorized = windexError {
                try? tokenStore.deleteToken(for: profile)
            }
            connectionState = .failed(profile, failure(from: error, profile: profile))
        }
    }

    /// Called only after `whoami` accepts the candidate token.
    func persistVerifiedToken(_ token: String?, for profile: ConnectionProfile) throws {
        if let token, !token.isEmpty {
            try tokenStore.saveToken(token, for: profile)
        } else {
            try tokenStore.deleteToken(for: profile)
        }
    }

    func handleClientError(_ error: any Error) {
        guard let profile = currentProfile else { return }
        if let windexError = error as? WindexError,
           case .unauthorized = windexError {
            try? tokenStore.deleteToken(for: profile)
            disconnectSession()
            client = nil
            connectionState = .failed(profile, .unauthorized)
        }
    }

    func retryConnection() async {
        guard let profile = currentProfile else { return }
        await connect(profile.displayAddress, useStoredToken: true)
    }

    func changeBackend() {
        addressStore.remove()
        backendAddress = ""
        disconnectSession()
        client = nil
        connectionState = .unconfigured
        selection = .overview
    }

    func forgetBackend() {
        if let profile = currentProfile {
            try? tokenStore.deleteToken(for: profile)
        }
        changeBackend()
    }

    private func failure(
        from error: any Error,
        profile: ConnectionProfile
    ) -> ConnectionFailure {
        guard let windexError = error as? WindexError else {
            return .unexpected(error.localizedDescription)
        }
        switch windexError {
        case .transport:
            return .unreachable(address: profile.displayAddress)
        case .unauthorized:
            return .unauthorized
        case .adminDisabled(let message):
            return .adminDisabled(message)
        case .http(_, let message), .notFound(let message):
            return .unexpected(message)
        case .conflict(let message), .preconditionFailed(let message),
             .preconditionRequired(let message):
            return .unexpected(message)
        case .unsupportedContractEpoch:
            return .incompatibleContract(windexError.localizedDescription)
        case .validation(_, let message):
            return .unexpected(message)
        case .decoding:
            return .unexpected(windexError.localizedDescription)
        case .invalidURL(let raw):
            return .invalidAddress(raw)
        }
    }

    private func disconnectSession() {
        session?.stop()
        session = nil
    }
}
