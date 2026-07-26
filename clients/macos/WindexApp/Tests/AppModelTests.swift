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

    @Test("composer renames Flows without losing their presentation")
    func composerRenamesFlowPresentation() {
        let model = PipelineComposerModel()
        model.newPipeline(registry: Self.registry)
        model.move("node", to: CGPoint(x: 320, y: 190))

        model.renameSelectedFlow(to: "Refresh Docs", registry: Self.registry)

        #expect(model.selectedFlow == "refresh_docs")
        #expect(model.draft?.flows.map(\.name) == ["refresh_docs"])
        #expect(model.positions["node"] == CGPoint(x: 320, y: 190))
    }

    @Test("composer connects Flow boundaries through compatible Nodes")
    func composerBoundaryConnections() throws {
        let model = PipelineComposerModel()
        model.newPipeline(registry: Self.registry)
        model.addBoundary(
            owner: .input,
            type: "documents",
            registry: Self.registry
        )
        model.add(Self.transformModule, registry: Self.registry)
        model.addBoundary(
            owner: .output,
            type: "documents",
            registry: Self.registry
        )
        let node = try #require(model.currentFlow?.nodes.first)

        model.beginConnection(from: .input("input_1"))
        #expect(model.canConnect(to: .node(node.id), registry: Self.registry))
        model.finishConnection(to: .node(node.id), registry: Self.registry)
        model.beginConnection(from: .node(node.id))
        #expect(model.canConnect(to: .output("output_1"), registry: Self.registry))
        model.finishConnection(to: .output("output_1"), registry: Self.registry)

        #expect(model.currentFlow?.edges == [
            .init(from: .input("input_1"), to: .node(node.id)),
            .init(from: .node(node.id), to: .output("output_1")),
        ])
    }

    @Test("canvas zoom is bounded and keeps drag geometry in graph coordinates")
    func canvasZoom() {
        let viewport = PipelineCanvasViewport()
        viewport.setZoom(4)
        #expect(viewport.zoom == 2)
        #expect(viewport.translatedPosition(
            origin: CGPoint(x: 100, y: 100),
            translation: CGSize(width: 40, height: 20)
        ) == CGPoint(x: 120, y: 110))
        viewport.setZoom(0.1)
        #expect(viewport.zoom == 0.5)
    }

    @Test("validation diagnostics focus their Flow, Node, and field")
    func composerDiagnosticFocus() throws {
        let model = PipelineComposerModel()
        model.newPipeline(registry: Self.registry)
        model.add(Self.transformModule, registry: Self.registry)
        let node = try #require(model.currentFlow?.nodes.first)
        let issue = PipelineValidationIssue(
            path: "flows.main.nodes.\(node.id).with.limit",
            code: "invalid_value",
            severity: .error,
            message: "Limit is invalid."
        )

        _ = model.focus(issue, registry: Self.registry)

        #expect(model.selectedFlow == "main")
        #expect(model.selectedNodeID == node.id)
        #expect(model.focusedFieldKey == "limit")
        #expect(model.rightPane == .inspector)
    }

    @Test("stale debounced server validation cannot replace current diagnostics")
    func composerValidationRevision() {
        let model = PipelineComposerModel()
        model.newPipeline(registry: Self.registry)
        let staleRevision = model.semanticRevision
        model.addBoundary(
            owner: .input,
            type: "documents",
            registry: Self.registry
        )
        let issue = Components.Schemas.ValidationIssueModel(
            code: "server",
            message: "Server issue",
            path: "flows.main",
            severity: .error
        )
        let response = PipelineValidationWire(issues: [issue], valid: false)

        model.applyServerValidation(response, revision: staleRevision)
        #expect(model.serverIssues.isEmpty)
        model.applyServerValidation(response, revision: model.semanticRevision)
        #expect(model.serverIssues.map(\.code) == ["server"])
    }

    @Test("Source settings share one draft and preserve it during reconciliation")
    func sharedSourceSettingsDraft() throws {
        let store = SourceSettingsDraftStore()
        let initial = try Self.settingsScope(value: 10)
        let changedOnServer = try Self.settingsScope(value: 30)
        let first = store.reconcile(initial)
        let second = store.reconcile(initial)
        #expect(first === second)

        first.set("batch", .int(20))
        let reconciled = store.reconcile(changedOnServer)
        #expect(reconciled === first)
        #expect(reconciled.value(forKey: "batch") == .int(20))

        let adopted = store.adopt(changedOnServer)
        #expect(adopted === first)
        #expect(adopted.value(forKey: "batch") == .int(30))
        #expect(!adopted.isDirty)
    }

    @Test("Source Run history is isolated and merges older pages")
    func sourceRunHistory() {
        let store = SharedRunStore()
        let reference = PipelineRevisionReference(
            pipeline: "crawl",
            version: 2,
            specHash: "hash"
        )
        store.replace([
            .init(id: 9, sourceName: "docs", pipeline: reference, state: .running, flow: "refresh"),
            .init(id: 8, sourceName: "other", pipeline: reference, state: .failed, flow: "refresh"),
        ])
        store.mergeSourceHistory(
            [.init(id: 7, sourceName: "docs", pipeline: reference, state: .succeeded, flow: "refresh")],
            source: "docs",
            replacing: false
        )

        #expect(store.runs(for: "docs").map(\.id) == [9, 7])
        #expect(store.runs(for: "other").map(\.id) == [8])
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

    @Test("Pipeline Run sheet mode distinguishes a normal Run from Dry Run")
    func pipelineRunMode() {
        #expect(!PipelineRunMode.run.isDryRun)
        #expect(PipelineRunMode.run.queueTitle == "Queue Run")
        #expect(PipelineRunMode.dryRun.isDryRun)
        #expect(PipelineRunMode.dryRun.queueTitle == "Queue Dry Run")
    }

    @Test("re-submitting the same query gives the newest request ownership")
    func repeatedSearchUsesRequestIdentity() async throws {
        let gate = SearchOperationGate()
        let model = SearchModel()
        let operation: SearchModel.SearchOperation = { query in
            try await gate.search(query)
        }

        model.query = "same query"
        let older = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("same query"))

        let newer = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("same query", count: 2))

        await gate.succeedNewest(
            try Self.searchResponse(
                query: "same query",
                id: "newer-result"
            ),
            request: "same query"
        )
        await newer.value
        #expect(model.response?.query == "same query")
        #expect(model.response?.results.first?.id == "newer-result")
        #expect(model.searchErrorMessage == nil)
        #expect(!model.isSearching)

        await gate.fail(
            WindexError.http(status: 503, message: "stale outage"),
            request: "same query"
        )
        await older.value
        #expect(model.response?.query == "same query")
        #expect(model.response?.results.first?.id == "newer-result")
        #expect(model.searchErrorMessage == nil)
        #expect(!model.isSearching)
    }

    @Test("a stale search response cannot replace a newer error")
    func staleSearchResponseIsIgnored() async throws {
        let gate = SearchOperationGate()
        let model = SearchModel()
        let operation: SearchModel.SearchOperation = { query in
            try await gate.search(query)
        }

        model.query = "older"
        let older = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("older"))

        model.query = "newer"
        let newer = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("newer"))

        await gate.fail(
            WindexError.http(status: 503, message: "current outage"),
            request: "newer"
        )
        await newer.value
        #expect(model.response == nil)
        #expect(model.searchErrorMessage == "current outage")
        #expect(!model.isSearching)

        await gate.succeed(
            try Self.searchResponse(
                query: "older",
                id: "stale-result"
            ),
            request: "older"
        )
        await older.value
        #expect(model.response == nil)
        #expect(model.searchErrorMessage == "current outage")
        #expect(!model.isSearching)
    }

    @Test("every search input change invalidates in-flight ownership")
    func searchInputChangesInvalidatePendingWork() async throws {
        let gate = SearchOperationGate()
        let model = SearchModel()
        let operation: SearchModel.SearchOperation = { query in
            try await gate.search(query)
        }
        model.query = "pending"

        let sourceTask = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("pending"))
        model.source = .wiki
        #expect(sourceTask.isCancelled)
        #expect(!model.isSearching)
        #expect(model.response == nil)
        await gate.succeed(
            try Self.searchResponse(query: "pending", id: "stale-source"),
            request: "pending"
        )
        await sourceTask.value
        #expect(model.response == nil)

        let modeTask = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("pending"))
        model.mode = .lexical
        #expect(modeTask.isCancelled)
        #expect(!model.isSearching)
        await gate.succeed(
            try Self.searchResponse(query: "pending", id: "stale-mode"),
            request: "pending"
        )
        await modeTask.value
        #expect(model.response == nil)

        let limitTask = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("pending"))
        model.limit = 30
        #expect(limitTask.isCancelled)
        #expect(!model.isSearching)
        await gate.succeed(
            try Self.searchResponse(query: "pending", id: "stale-limit"),
            request: "pending"
        )
        await limitTask.value
        #expect(model.response == nil)

        let queryTask = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("pending"))
        model.query = "   "
        #expect(queryTask.isCancelled)
        #expect(!model.isSearching)
        #expect(model.submit(using: operation) == nil)
        await gate.succeed(
            try Self.searchResponse(query: "pending", id: "stale-query"),
            request: "pending"
        )
        await queryTask.value
        #expect(model.response == nil)
        #expect(model.searchErrorMessage == nil)
    }

    @Test("current degraded and unavailable searches have deterministic state")
    func currentSearchOutcomesAndDisappearance() async throws {
        let gate = SearchOperationGate()
        let model = SearchModel()
        let operation: SearchModel.SearchOperation = { query in
            try await gate.search(query)
        }

        model.query = "degraded"
        let degraded = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("degraded"))
        await gate.succeed(
            try Self.searchResponse(
                query: "degraded",
                id: "partial-result",
                mode: "hybrid (partial: wiki unavailable)"
            ),
            request: "degraded"
        )
        await degraded.value
        #expect(model.response?.isDegraded == true)
        #expect(model.searchErrorMessage == nil)
        #expect(!model.isSearching)

        model.query = "unavailable"
        let unavailable = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("unavailable"))
        await gate.fail(
            WindexError.http(status: 503, message: "search unavailable"),
            request: "unavailable"
        )
        await unavailable.value
        #expect(model.response == nil)
        #expect(model.searchErrorMessage == "search unavailable")
        #expect(!model.isSearching)

        model.query = "leaving"
        let leaving = try #require(model.submit(using: operation))
        #expect(await gate.waitForRequest("leaving"))
        model.cancelPending()
        #expect(leaving.isCancelled)
        #expect(!model.isSearching)
        await gate.succeed(
            try Self.searchResponse(query: "leaving", id: "too-late"),
            request: "leaving"
        )
        await leaving.value
        #expect(model.response == nil)
        #expect(model.searchErrorMessage == nil)
    }

    @Test("Source upgrade editor confirms only the latest server candidate")
    func editableSourceUpgradeCandidate() throws {
        let parameter = try PipelineParameterDefinition(
            key: "batch",
            kind: .int,
            title: "Batch",
            required: true
        ).parameter()
        let editor = SourceUpgradeEditorModel()
        #expect(editor.applyIfCurrent(
            try Self.upgradePreview(
                candidate: 20,
                valid: true,
                token: "candidate-20"
            ),
            parameters: [parameter],
            requestedVersion: 5,
            selectedVersion: 5
        ))
        #expect(editor.canConfirm)

        editor.candidateForm?.set("batch", .int(24))
        #expect(editor.hasUnpreviewedChanges)
        #expect(editor.valuesForPreview == ["batch": 24])
        #expect(editor.confirmation == nil)

        let invalid = try Self.upgradePreview(
            candidate: 24,
            valid: false,
            token: nil,
            issueMessage: "Batch 24 is unavailable."
        )
        #expect(editor.applyIfCurrent(
            invalid,
            parameters: [parameter],
            requestedVersion: 5,
            selectedVersion: 5
        ))
        #expect(editor.candidateForm?.value(forKey: "batch") == .int(24))
        #expect(editor.preview?.issues.first?.code == "candidate_invalid")
        #expect(!editor.canConfirm)

        editor.candidateForm?.set("batch", .int(16))
        #expect(editor.valuesForPreview == ["batch": 16])
        #expect(editor.applyIfCurrent(
            try Self.upgradePreview(
                candidate: 16,
                valid: true,
                token: "candidate-16"
            ),
            parameters: [parameter],
            requestedVersion: 5,
            selectedVersion: 5
        ))

        let confirmation = try #require(editor.confirmation)
        #expect(confirmation.version == 5)
        #expect(confirmation.values == ["batch": 16])
        #expect(confirmation.token == "candidate-16")
    }

    @Test("stale Source upgrade preview cannot replace a newly selected revision")
    func staleSourceUpgradePreview() throws {
        let parameter = try PipelineParameterDefinition(
            key: "batch",
            kind: .int,
            title: "Batch"
        ).parameter()
        let editor = SourceUpgradeEditorModel()

        #expect(editor.applyIfCurrent(
            try Self.upgradePreview(
                targetVersion: 6,
                candidate: 16,
                valid: true,
                token: "candidate-v6"
            ),
            parameters: [parameter],
            requestedVersion: 6,
            selectedVersion: 6
        ))

        #expect(!editor.applyIfCurrent(
            try Self.upgradePreview(
                targetVersion: 5,
                candidate: 24,
                valid: true,
                token: "stale-v5"
            ),
            parameters: [parameter],
            requestedVersion: 5,
            selectedVersion: 6
        ))
        #expect(editor.preview?.targetVersion == 6)
        #expect(editor.candidateForm?.value(forKey: "batch") == .int(16))
        #expect(editor.confirmation?.token == "candidate-v6")
    }

    @Test("BackendSession forwards a memory partition to WindexClient")
    func backendSessionForwardsIngestPartition() async throws {
        IngestRecordingURLProtocol.configure()
        defer { IngestRecordingURLProtocol.reset() }
        let profile = try ConnectionProfile("http://windex.test")
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [IngestRecordingURLProtocol.self]
        let client = WindexClient(
            configuration: .init(baseURL: profile.baseURL),
            token: "token",
            session: URLSession(configuration: configuration)
        )
        let session = BackendSession(
            client: client,
            backend: ConnectedBackend(
                profile: profile,
                evidence: Self.evidence,
                hasStoredToken: true
            )
        )
        let conversation = "0f9d2a41-3c7e-4b18-9a05-6d1f8c2e4b77"
        let batch = try MemoryIngestBatch.replacement(
            conversationID: conversation,
            chunks: [
                try MemoryConversationChunk(
                    chunkIndex: 4,
                    messageRangeStart: 8,
                    messageRangeEnd: 12,
                    text: "Remember this"
                ),
            ]
        )

        try await session.ingest(
            batch.documents,
            source: "memory",
            mode: batch.mode,
            partition: batch.partition,
            idempotencyKey: "backend-session-memory"
        )

        let request = try #require(
            IngestRecordingURLProtocol.requests.first {
                $0.method == "POST"
                    && $0.path == "/v1/sources/memory/ingest"
            }
        )
        let body = try #require(
            try JSONDecoder().decode(
                JSONValue.self,
                from: request.body
            ).objectValue
        )
        #expect(body["mode"] == .string("full"))
        #expect(body["partition"] == .string(conversation))
        #expect(body["documents"]?.arrayValue?.count == 1)
    }

    @Test("BackendSession preserves structured memory validation errors")
    func backendSessionSurfacesMemoryValidation() async throws {
        IngestRecordingURLProtocol.configure(
            status: 422,
            body: """
            {"detail":[{
              "loc":["body","partition"],
              "msg":"memory push requires a conversation partition",
              "type":"missing"
            }]}
            """
        )
        defer { IngestRecordingURLProtocol.reset() }
        let profile = try ConnectionProfile("http://windex.test")
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [IngestRecordingURLProtocol.self]
        let client = WindexClient(
            configuration: .init(baseURL: profile.baseURL),
            token: "token",
            session: URLSession(configuration: configuration)
        )
        let session = BackendSession(
            client: client,
            backend: ConnectedBackend(
                profile: profile,
                evidence: Self.evidence,
                hasStoredToken: true
            )
        )

        do {
            try await session.ingest(
                [],
                source: "memory",
                mode: "full",
                idempotencyKey: "backend-session-invalid"
            )
            Issue.record("expected validation failure")
        } catch let error as WindexError {
            guard case .validation(let failures, _) = error else {
                Issue.record("wrong error: \(error)")
                return
            }
            #expect(failures.map(\.field) == ["partition"])
            #expect(error.localizedDescription.contains("conversation partition"))
        }
    }

    @Test("unavailable Source Modules block new Runs and ingestion")
    func unavailableSourceModulesBlockNewWork() async throws {
        IngestRecordingURLProtocol.configure()
        defer { IngestRecordingURLProtocol.reset() }
        let profile = try ConnectionProfile("http://windex.test")
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [IngestRecordingURLProtocol.self]
        let client = WindexClient(
            configuration: .init(baseURL: profile.baseURL),
            token: "token",
            session: URLSession(configuration: configuration)
        )
        let session = BackendSession(
            client: client,
            backend: ConnectedBackend(
                profile: profile,
                evidence: Self.evidence,
                hasStoredToken: true
            )
        )
        let moduleStatus = SourceModuleStatusWire(
            available: false,
            latestPipelineVersion: 4,
            pipelineRevisionId: 21,
            pipelineVersion: 3,
            source: "memory",
            unavailableModules: ["push.docs"],
            upgradeRequired: true
        )
        session.sources.replaceModuleDiagnostics(
            health: ModuleHealthWire(
                sources: [moduleStatus],
                status: .degraded,
                strandedSources: 1
            ),
            statuses: [moduleStatus]
        )

        #expect(session.sources.moduleStatus(for: "memory") == moduleStatus)
        await #expect(throws: SourceModuleUnavailableError.self) {
            try await session.runLatest(source: "memory")
        }
        await #expect(throws: SourceModuleUnavailableError.self) {
            try await session.ingest(
                [],
                source: "memory",
                mode: "full",
                partition: "conversation-1",
                idempotencyKey: "blocked-memory-ingest"
            )
        }
        #expect(IngestRecordingURLProtocol.requests.isEmpty)
    }

    private static let evidence = PairingEvidence(
        version: "0.1.0",
        uptimeSeconds: 128,
        authRequired: true,
        scopes: ["admin"])

    private static let transformModule = PipelineModuleDescriptor(
        id: "test.transform",
        kind: "transform",
        version: "1",
        title: "Transform"
    )

    private static let registry = PipelineRegistry(
        version: 1,
        portTypes: [.init(name: "documents", title: "Documents")],
        kinds: [
            .init(
                id: "transform",
                title: "Transform",
                description: "",
                inputType: "documents",
                outputType: "documents",
                stateful: false
            ),
        ],
        modules: [transformModule]
    )

    private static func settingsScope(value: Int) throws -> SettingsScope {
        try JSONDecoder().decode(
            SettingsScope.self,
            from: Data(
                """
                {
                  "scope": "docs",
                  "fields": [{
                    "key": "batch",
                    "kind": "int",
                    "title": "Batch",
                    "value": \(value),
                    "origin": "source"
                  }]
                }
                """.utf8
            )
        )
    }

    private static func searchResponse(
        query: String,
        id: String,
        mode: String = "hybrid"
    ) throws -> SearchResponse {
        let object: [String: Any] = [
            "query": query,
            "results": [
                [
                    "id": id,
                    "score": 0.9,
                    "source": "test",
                ],
            ],
            "mode": mode,
            "took_ms": 4,
            "timings": ["search": 3.5],
        ]
        let data = try JSONSerialization.data(withJSONObject: object)
        return try JSONDecoder().decode(SearchResponse.self, from: data)
    }

    private static func upgradePreview(
        targetVersion: Int = 5,
        candidate: Int,
        valid: Bool,
        token: String?,
        issueMessage: String? = nil
    ) throws -> SourceUpgradePreviewWire {
        let confirmationToken = token.map { "\"\($0)\"" } ?? "null"
        let issues = issueMessage.map {
            """
            [{
              "code": "candidate_invalid",
              "message": "\($0)",
              "path": "values.batch",
              "severity": "error"
            }]
            """
        } ?? "[]"
        return try JSONDecoder().decode(
            SourceUpgradePreviewWire.self,
            from: Data(
                """
                {
                  "source_id": 1,
                  "from_version": 4,
                  "target_version": \(targetVersion),
                  "target_hash": "target-hash",
                  "expected_etag": "source-v4",
                  "candidate_hash": "candidate-\(candidate)",
                  "candidate": {"batch": \(candidate)},
                  "retained": {},
                  "defaulted": {},
                  "removed": [],
                  "clamped": {},
                  "missing": [],
                  "install_stage_changed": [],
                  "state_impact": {},
                  "issues": \(issues),
                  "valid": \(valid),
                  "confirmation_token": \(confirmationToken)
                }
                """.utf8
            )
        )
    }
}

/// A manually completed transport which deliberately ignores task
/// cancellation. It exercises the generation check rather than relying on
/// URLSession to abort promptly.
private actor SearchOperationGate {
    private typealias Continuation =
        CheckedContinuation<SearchResponse, any Error>
    private var requests: [String: [Continuation]] = [:]

    func search(_ query: SearchQuery) async throws -> SearchResponse {
        try await withCheckedThrowingContinuation { continuation in
            requests[query.q, default: []].append(continuation)
        }
    }

    func waitForRequest(_ query: String, count: Int = 1) async -> Bool {
        for _ in 0..<10_000 {
            if (requests[query]?.count ?? 0) >= count {
                return true
            }
            await Task.yield()
        }
        return false
    }

    func succeed(_ response: SearchResponse, request query: String) {
        take(query).resume(returning: response)
    }

    func succeedNewest(_ response: SearchResponse, request query: String) {
        take(query, newest: true).resume(returning: response)
    }

    func fail(_ error: WindexError, request query: String) {
        take(query).resume(throwing: error)
    }

    private func take(
        _ query: String,
        newest: Bool = false
    ) -> Continuation {
        guard var pending = requests[query], !pending.isEmpty else {
            preconditionFailure("No pending search request for \(query)")
        }
        let continuation: Continuation
        if newest {
            continuation = pending.removeLast()
        } else {
            continuation = pending.removeFirst()
        }
        requests[query] = pending
        return continuation
    }
}

private struct RecordedIngestRequest: Sendable {
    let method: String
    let path: String
    let body: Data
}

private final class IngestRecordingURLProtocol: URLProtocol, @unchecked Sendable {
    private static let lock = NSLock()
    nonisolated(unsafe) private static var recorded: [RecordedIngestRequest] = []
    nonisolated(unsafe) private static var responseStatus = 202
    nonisolated(unsafe) private static var responseBody = Data()

    static var requests: [RecordedIngestRequest] {
        lock.withLock { recorded }
    }

    static func configure(
        status: Int = 202,
        body: String = #"{"run_id":31,"queued":true,"coalesced":false,"rerun_of":null}"#
    ) {
        lock.withLock {
            recorded = []
            responseStatus = status
            responseBody = Data(body.utf8)
        }
    }

    static func reset() {
        lock.withLock {
            recorded = []
            responseStatus = 202
            responseBody = Data()
        }
    }

    override class func canInit(with request: URLRequest) -> Bool { true }

    override class func canonicalRequest(
        for request: URLRequest
    ) -> URLRequest {
        request
    }

    override func startLoading() {
        let body = Self.bodyData(for: request)
        let snapshot = Self.lock.withLock { () -> (Int, Data) in
            Self.recorded.append(
                RecordedIngestRequest(
                    method: request.httpMethod ?? "GET",
                    path: request.url?.path ?? "",
                    body: body
                )
            )
            if request.httpMethod == "POST",
               request.url?.path == "/v1/sources/memory/ingest" {
                return (Self.responseStatus, Self.responseBody)
            }
            return (404, Data(#"{"detail":"not configured"}"#.utf8))
        }
        guard let url = request.url,
              let response = HTTPURLResponse(
                url: url,
                statusCode: snapshot.0,
                httpVersion: "HTTP/1.1",
                headerFields: ["Content-Type": "application/json"]
              ) else {
            client?.urlProtocol(
                self,
                didFailWithError: URLError(.badServerResponse)
            )
            return
        }
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: snapshot.1)
        client?.urlProtocolDidFinishLoading(self)
    }

    private static func bodyData(for request: URLRequest) -> Data {
        if let body = request.httpBody {
            return body
        }
        guard let stream = request.httpBodyStream else {
            return Data()
        }
        stream.open()
        defer { stream.close() }
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while true {
            let count = stream.read(&buffer, maxLength: buffer.count)
            guard count > 0 else { break }
            data.append(buffer, count: count)
        }
        return data
    }

    override func stopLoading() {}
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
