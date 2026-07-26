import Foundation
import Testing
import WindexKit
@testable import Windex

@Suite("App model")
@MainActor
struct AppModelTests {

    @Test("a bare LAN host becomes an HTTP base URL")
    func profileNormalizesAddress() throws {
        let profile = try ConnectionProfile(" spark.local:8100/admin/v1 ")
        #expect(profile.baseURL.absoluteString == "http://spark.local:8100")
        #expect(profile.credentialAccount == "http://spark.local:8100")
    }

    @Test("non-HTTP schemes are rejected")
    func profileRejectsOtherSchemes() {
        #expect(throws: ConnectionProfileError.self) {
            _ = try ConnectionProfile("ftp://spark.local:8100")
        }
    }

    @Test("restoring proves the Keychain token before becoming ready")
    func restoreProvesStoredToken() async throws {
        let profile = try ConnectionProfile("http://spark.local:8100")
        let tokens = MemoryTokenStore(values: [profile.credentialAccount: "secret"])
        let addresses = MemoryAddressStore(value: profile.displayAddress)
        let recorder = PairingRecorder(result: .paired(Self.evidence))
        let model = AppModel(
            tokenStore: tokens,
            addressStore: addresses,
            pairClient: recorder.pair)

        await model.restore()

        #expect(model.backendAddress == "http://spark.local:8100")
        #expect(model.connectedBackend?.profile == profile)
        #expect(model.connectedBackend?.hasStoredToken == true)
        #expect(model.session != nil)
        #expect(tokens.loadedAccounts == [profile.credentialAccount])
        #expect(recorder.tokens == ["secret"])
    }

    @Test("a token is saved only after whoami accepts it")
    func verifiedTokenLifecycle() async throws {
        let profile = try ConnectionProfile("https://windex.example")
        let tokens = MemoryTokenStore()
        let addresses = MemoryAddressStore()
        let recorder = PairingRecorder(result: .paired(Self.evidence))
        let model = AppModel(
            tokenStore: tokens,
            addressStore: addresses,
            pairClient: recorder.pair)

        await model.connect(profile.displayAddress, candidateToken: "accepted")

        #expect(tokens.values[profile.credentialAccount] == "accepted")
        #expect(model.connectedBackend?.hasStoredToken == true)
        #expect(addresses.value == profile.displayAddress)

        model.forgetBackend()
        #expect(tokens.values[profile.credentialAccount] == nil)
        #expect(addresses.value == nil)
        #expect(model.session == nil)
        #expect(model.connectionState == .unconfigured)
    }

    @Test("a token-required response does not persist the typed credential")
    func tokenRequiredDoesNotPersist() async throws {
        let profile = try ConnectionProfile("spark.local:8100")
        let tokens = MemoryTokenStore()
        let recorder = PairingRecorder(result: .tokenRequired)
        let model = AppModel(
            tokenStore: tokens,
            addressStore: MemoryAddressStore(),
            pairClient: recorder.pair)

        await model.connect(profile.displayAddress)

        #expect(model.connectionState == .tokenRequired(profile))
        #expect(model.session == nil)
        #expect(tokens.values.isEmpty)
    }

    @Test("a rejected stored token is removed and returns to pairing")
    func rejectedStoredTokenIsRemoved() async throws {
        let profile = try ConnectionProfile("spark.local:8100")
        let tokens = MemoryTokenStore(values: [profile.credentialAccount: "stale"])
        let recorder = PairingRecorder(result: .unauthorized)
        let model = AppModel(
            tokenStore: tokens,
            addressStore: MemoryAddressStore(value: profile.displayAddress),
            pairClient: recorder.pair)

        await model.restore()

        #expect(tokens.values[profile.credentialAccount] == nil)
        #expect(model.session == nil)
        #expect(model.connectionState == .failed(profile, .unauthorized))
    }

    @Test("workspace routes preserve exact revision and Console context")
    func workspaceRoutes() {
        let model = AppModel(
            tokenStore: MemoryTokenStore(),
            addressStore: MemoryAddressStore()
        )
        let reference = PipelineRevisionReference(
            pipeline: "crawl",
            version: 7,
            specHash: "sha-7"
        )

        model.openPipeline(reference, flow: "discover")
        #expect(model.selection == .pipelines)
        #expect(model.pipelineNavigation == .init(
            reference: reference,
            flow: "discover"
        ))

        model.createSource(using: reference)
        #expect(model.selection == .sources)
        #expect(model.sourceCreationRevision == reference)

        model.openRun(42)
        #expect(model.selection == .runs)
        #expect(model.selectedRunID == 42)

        let filter = OperationalEventFilter(
            sourceName: "docs",
            pipelineName: "crawl",
            runID: 42,
            node: "extract"
        )
        model.openConsole(filter)
        #expect(model.selection == .logs)
        #expect(model.consoleFilterRequest == filter)
    }

    @Test("composer preserves independent unsaved Flow layouts")
    func composerFlowLayouts() {
        let model = PipelineComposerModel()
        model.newPipeline(registry: nil)
        model.move("first", to: CGPoint(x: 210, y: 130))

        model.addFlow()
        #expect(model.selectedFlow == "flow_2")
        #expect(model.positions.isEmpty)
        model.move("second", to: CGPoint(x: 480, y: 270))
        model.stashLayout(for: "flow_2")

        model.selectedFlow = "main"
        model.apply(nil)
        #expect(model.positions["first"] == CGPoint(x: 210, y: 130))
        #expect(model.positions["second"] == nil)

        model.stashLayout(for: "main")
        model.selectedFlow = "flow_2"
        model.apply(nil)
        #expect(model.positions["second"] == CGPoint(x: 480, y: 270))
        #expect(model.positions["first"] == nil)
    }

    @Test("Console history tracks the server forward cursor")
    func consoleHistoryCursor() {
        let store = SharedLogStore()

        store.loadingHistory(reset: true)
        store.loadedHistory(nextCursor: 500, count: 500)
        #expect(store.historyCursor == 500)
        #expect(store.historyHasMore)

        store.loadingHistory(reset: false)
        store.loadedHistory(nextCursor: 610, count: 110)
        #expect(store.historyCursor == 610)
        #expect(store.historyHasMore)

        store.loadingHistory(reset: false)
        store.loadedHistory(nextCursor: 610, count: 0)
        #expect(store.historyCursor == 610)
        #expect(!store.historyHasMore)
    }

    private static let evidence = PairingEvidence(
        version: "0.1.0",
        uptimeSeconds: 128,
        authRequired: true,
        scopes: ["admin"])
}

private final class PairingRecorder: @unchecked Sendable {
    private let result: PairingOutcome
    private(set) var tokens: [String?] = []

    init(result: PairingOutcome) {
        self.result = result
    }

    var pair: AppModel.PairingOperation {
        { [self] _, token in
            tokens.append(token)
            return result
        }
    }
}

private final class MemoryTokenStore: TokenStoring, @unchecked Sendable {
    var values: [String: String]
    var loadedAccounts: [String] = []

    init(values: [String: String] = [:]) {
        self.values = values
    }

    func loadToken(for profile: ConnectionProfile) throws -> String? {
        loadedAccounts.append(profile.credentialAccount)
        return values[profile.credentialAccount]
    }

    func saveToken(_ token: String, for profile: ConnectionProfile) throws {
        values[profile.credentialAccount] = token
    }

    func deleteToken(for profile: ConnectionProfile) throws {
        values[profile.credentialAccount] = nil
    }
}

private final class MemoryAddressStore: BackendAddressStoring, @unchecked Sendable {
    var value: String?

    init(value: String? = nil) {
        self.value = value
    }

    func load() -> String? { value }
    func save(_ address: String) { value = address }
    func remove() { value = nil }
}
