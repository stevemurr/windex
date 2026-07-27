import Dispatch
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

    @Test("control event classification preserves exact affected scopes")
    func controlEventScopeClassification() {
        let scheduled = OperationalEvent(
            sequence: 1,
            timestamp: .now,
            level: .info,
            component: "run",
            sourceName: "docs",
            pipelineName: "crawl",
            pipelineVersion: 3,
            runID: 42,
            event: "run.queued",
            message: "",
            data: ["trigger": .string("schedule")]
        )
        let published = OperationalEvent(
            sequence: 2,
            timestamp: .now,
            level: .info,
            component: "pipeline",
            pipelineName: "crawl",
            pipelineVersion: 3,
            event: "pipeline.revision_published",
            message: ""
        )
        let module = OperationalEvent(
            sequence: 3,
            timestamp: .now,
            level: .info,
            component: "module_admin",
            module: "custom.clean",
            event: "module.approved",
            message: ""
        )

        var plan = ControlReconciliationPlan(event: scheduled)
        plan.formUnion(.init(event: published))
        plan.formUnion(.init(event: module))

        #expect(plan.refreshOverview)
        #expect(!plan.refreshAll)
        #expect(plan.refreshRegistry)
        #expect(plan.refreshModuleDiagnostics)
        #expect(plan.runs == [42])
        #expect(plan.sourceStatuses == ["docs"])
        #expect(plan.sourceTriggers == ["docs"])
        #expect(plan.sourceDetails.isEmpty)
        #expect(plan.pipelines == [
            .init(name: "crawl", version: 3),
        ])

        let unknown = ControlReconciliationPlan(event: .init(
            sequence: 4,
            timestamp: .now,
            level: .info,
            component: "future_control_domain",
            event: "future.changed",
            message: ""
        ))
        #expect(unknown.refreshAll)
    }

    @Test("control event bursts reconcile unique resources without a full reload")
    func targetedControlReconciliation() async throws {
        let host = "reconciliation.windex.test"
        ControlReconciliationURLProtocol.configure(host: host)
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let profile = try ConnectionProfile("http://\(host)")
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [ControlReconciliationURLProtocol.self]
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
            ),
            reconciliationDelay: .milliseconds(10)
        )
        session.runs.replace([])
        session.sources.replace([
            SourceDeployment(
                name: "docs",
                title: "Docs",
                origin: "push",
                pipeline: .init(
                    pipeline: "crawl",
                    version: 2,
                    specHash: "hash-2"
                ),
                search: .init(
                    searchName: "docs",
                    idPrefix: "docs:",
                    collectionKey: "docs",
                    searchProfile: "generic",
                    includeInAll: true
                ),
                stateNamespace: "docs"
            ),
        ])
        let runEvent = OperationalEvent(
            sequence: 1,
            timestamp: .now,
            level: .info,
            component: "worker",
            sourceName: "docs",
            pipelineName: "crawl",
            pipelineVersion: 3,
            runID: 42,
            taskID: 7,
            event: "task.leased",
            message: ""
        )
        let pipelineEvent = OperationalEvent(
            sequence: 2,
            timestamp: .now,
            level: .info,
            component: "pipeline",
            pipelineName: "crawl",
            pipelineVersion: 3,
            event: "pipeline.revision_published",
            message: ""
        )

        // A hundred journal rows collapse to one request per unique projection.
        for _ in 0..<50 {
            session.scheduleReconciliation(for: runEvent)
            session.scheduleReconciliation(for: pipelineEvent)
        }
        await session.waitForScheduledReconciliation()

        let paths = ControlReconciliationURLProtocol.requests(host: host).map(\.path)
        #expect(paths.count == 5)
        #expect(Dictionary(grouping: paths, by: { $0 }).mapValues(\.count) == [
            "/admin/v1/overview": 1,
            "/admin/v1/pipelines/crawl": 1,
            "/admin/v1/pipelines/crawl/revisions/3": 1,
            "/admin/v1/runs/42": 1,
            "/admin/v1/sources/docs/status": 1,
        ])
        #expect(!paths.contains { path in
            path.contains("/settings")
                || path.contains("/triggers")
                || path.contains("/log-events")
                || path == "/admin/v1/runs"
                || path == "/admin/v1/sources"
                || path == "/admin/v1/pipelines"
        })
        #expect(session.runs.runs.first?.id == 42)
        #expect(session.runs.runs.first?.state == .running)
        #expect(session.sources.sources.first?.status.currentRun?.id == 42)
        #expect(session.sources.sources.first?.status.activity == .running)
        #expect(session.pipelines.pipelines.first?.headVersion == 3)
        #expect(session.pipelines.revisions["crawl"]?.map(\.reference.version) == [3])
        #expect(session.overview.snapshot?.revision == 7)
    }

    @Test("a Source lifecycle event adds its complete bounded projection")
    func targetedNewSourceReconciliation() async throws {
        let host = "source-reconciliation.windex.test"
        ControlReconciliationURLProtocol.configure(host: host)
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let profile = try ConnectionProfile("http://\(host)")
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [ControlReconciliationURLProtocol.self]
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
            ),
            reconciliationDelay: .milliseconds(10)
        )
        session.runs.replace([])
        session.sources.replace([])
        session.pipelines.replace([
            PipelineSummary(
                name: "crawl",
                title: "Crawl",
                headVersion: 3,
                headHash: "hash-3",
                deploymentCount: 0
            ),
        ])
        let event = OperationalEvent(
            sequence: 9,
            timestamp: .now,
            level: .info,
            component: "source",
            sourceName: "newdocs",
            pipelineName: "crawl",
            pipelineVersion: 3,
            event: "source.changed",
            message: ""
        )

        session.scheduleReconciliation(for: event)
        await session.waitForScheduledReconciliation()

        let paths = ControlReconciliationURLProtocol.requests(host: host).map(\.path)
        #expect(paths.count == 8)
        #expect(Set(paths) == [
            "/admin/v1/module-health",
            "/admin/v1/overview",
            "/admin/v1/sources/newdocs",
            "/admin/v1/sources/newdocs/module-status",
            "/admin/v1/sources/newdocs/runs",
            "/admin/v1/sources/newdocs/settings",
            "/admin/v1/sources/newdocs/status",
            "/admin/v1/sources/newdocs/triggers",
        ])
        let source = try #require(session.sources.sources.first)
        #expect(source.name == "newdocs")
        #expect(source.pipeline.version == 3)
        #expect(source.generation == 2)
        #expect(source.configuration.effectiveValues["batch"] == .int(16))
        #expect(source.configuration.valuesHash == "settings-v3")
        #expect(session.sourceSettingsDrafts.form(for: "newdocs")?
            .value(forKey: "batch") == .int(16))
        #expect(session.sources.moduleStatus(for: "newdocs")?.available == true)
        #expect(session.pipelines.pipelines.first?.deploymentCount == 1)
    }

    @Test("a broken Source does not poison healthy Source projections")
    func sourceRefreshIsolatesProjectionFailures() async throws {
        let host = "source-isolation.windex.test"
        var responses = Self.sourceIsolationResponses(
            memorySettingsStatus: 503
        )
        responses["/admin/v1/sources/docs"] = .json(
            503,
            #"{"detail":"docs detail temporarily unavailable"}"#
        )
        responses["/admin/v1/sources/docs/runs"] = .json(
            503,
            #"{"detail":"docs history temporarily unavailable"}"#
        )
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: responses
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)

        await session.refreshAll()

        #expect(session.sources.state == .loaded)
        #expect(session.sources.sources.map(\.name) == ["docs"])
        #expect(session.sources.settingsETags == [
            "docs": "docs-settings-v2",
        ])
        #expect(session.sources.triggers["docs"]?.map(\.id) == [22])
        #expect(session.sourceSettingsDrafts.form(for: "docs")?
            .value(forKey: "batch") == .int(16))
        #expect(session.sourceSettingsDrafts.form(for: "memory") == nil)
        let docsDiagnostic = try #require(
            session.sources.diagnostic(for: "docs")
        )
        #expect(Set(docsDiagnostic.failures.keys) == [.detail, .history])
        #expect(!docsDiagnostic.usingLastKnownGood)
        #expect(docsDiagnostic.snapshotAvailable)
        let diagnostic = try #require(
            session.sources.diagnostic(for: "memory")
        )
        #expect(Set(diagnostic.failures.keys) == [.settings])
        #expect(!diagnostic.usingLastKnownGood)
        #expect(!diagnostic.snapshotAvailable)
        #expect(diagnostic.message.contains("Retry the refresh"))

        let paths = ControlReconciliationURLProtocol.requests(host: host)
            .map(\.path)
        for source in ["docs", "memory"] {
            #expect(paths.filter {
                $0 == "/admin/v1/sources/\(source)/settings"
            }.count == 1)
            #expect(paths.filter {
                $0 == "/admin/v1/sources/\(source)/triggers"
            }.count == 1)
            #expect(paths.filter {
                $0 == "/admin/v1/sources/\(source)/status"
            }.count == 1)
        }
    }

    @Test("a broken Source retains and then replaces one coherent snapshot")
    func sourceRefreshRetainsAndRecoversCoherentSnapshot() async throws {
        let host = "source-recovery.windex.test"
        var failedResponses = Self.sourceIsolationResponses(
            memorySettingsStatus: 503
        )
        failedResponses["/admin/v1/sources/memory/triggers"] = .json(
            503,
            #"{"detail":"memory triggers temporarily unavailable"}"#
        )
        failedResponses["/admin/v1/sources/memory/status"] = .json(
            503,
            #"{"detail":"memory status temporarily unavailable"}"#
        )
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: failedResponses
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)
        let oldTrigger = try Self.sourceTrigger(id: 11, hour: 9)
        let oldMemory = SourceDeployment(
            name: "memory",
            title: "Memory v1",
            origin: "push",
            pipeline: .init(
                pipeline: "memory",
                version: 1,
                specHash: "memory-hash-1"
            ),
            search: .init(
                searchName: "memory",
                idPrefix: "memory:",
                collectionKey: "memory",
                searchProfile: "memory",
                includeInAll: true
            ),
            stateNamespace: "memory",
            generation: 1,
            configuration: .init(
                configuredValues: ["batch": .int(10)],
                effectiveValues: ["batch": .int(10)],
                valuesHash: "memory-settings-v1"
            )
        )
        session.sources.replace([oldMemory])
        session.sources.setSettingsETag(
            "memory-settings-v1",
            for: "memory"
        )
        session.sources.setTriggers([oldTrigger], for: "memory")
        session.sourceSettingsDrafts.reconcile(
            try Self.sourceSettingsScope(source: "memory", value: 10)
        )

        await session.refreshAll()

        let retained = try #require(
            session.sources.sources.first { $0.name == "memory" }
        )
        #expect(retained.generation == 1)
        #expect(retained.title == "Memory v1")
        #expect(retained.configuration.valuesHash == "memory-settings-v1")
        #expect(session.sources.settingsETags["memory"]
            == "memory-settings-v1")
        #expect(session.sources.triggers["memory"]?.map(\.id) == [11])
        #expect(session.sourceSettingsDrafts.form(for: "memory")?
            .value(forKey: "batch") == .int(10))
        let retainedDiagnostic = try #require(
            session.sources.diagnostic(for: "memory")
        )
        #expect(Set(retainedDiagnostic.failures.keys)
            == [.settings, .triggers, .status])
        #expect(retainedDiagnostic.usingLastKnownGood)

        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: Self.sourceIsolationResponses(
                memorySettingsStatus: 200
            )
        )
        await session.refreshAll()

        let recovered = try #require(
            session.sources.sources.first { $0.name == "memory" }
        )
        #expect(recovered.generation == 2)
        #expect(recovered.title == "Memory")
        #expect(recovered.configuration.effectiveValues["batch"] == .int(20))
        #expect(recovered.configuration.valuesHash == "memory-settings-v2")
        #expect(session.sources.settingsETags["memory"]
            == "memory-settings-v2")
        #expect(session.sources.triggers["memory"]?.map(\.id) == [22])
        #expect(session.sourceSettingsDrafts.form(for: "memory")?
            .value(forKey: "batch") == .int(20))
        #expect(session.sources.diagnostic(for: "memory") == nil)
        #expect(session.sources.state == .loaded)
    }

    @Test("broken Pipeline revisions and layouts do not poison later Pipelines")
    func pipelineRefreshIsolatesRevisionAndLayoutFailures() async throws {
        let host = "pipeline-isolation.windex.test"
        var responses: [String: StubbedControlResponse] = [
            "/admin/v1/pipelines": .json(
                200,
                Self.pipelineListJSON([
                    ("decode", "Decode", 2),
                    ("headless", "Headless", 5),
                    ("identity", "Identity", 4),
                    ("layout", "Layout", 2),
                    ("omega", "Omega", 3),
                ])
            ),
            "/admin/v1/pipelines/decode/revisions": .json(
                200,
                Self.pipelineRevisionsJSON([
                    Self.pipelineRevisionJSON(
                        name: "decode",
                        version: 1,
                        flows: ["discover"],
                        validSpec: false
                    ),
                    Self.pipelineRevisionJSON(
                        name: "decode",
                        version: 2,
                        flows: ["discover"]
                    ),
                ])
            ),
            "/admin/v1/pipelines/decode/revisions/2/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "decode-layout-v2",
                    x: 20
                )
            ),
            "/admin/v1/pipelines/headless/revisions": .json(
                200,
                Self.pipelineRevisionsJSON(
                    name: "headless",
                    version: 4,
                    flows: ["discover"]
                )
            ),
            "/admin/v1/pipelines/headless/revisions/4/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "headless-layout-v4",
                    x: 40
                )
            ),
            "/admin/v1/pipelines/identity/revisions": .json(
                200,
                Self.pipelineRevisionsJSON([
                    Self.pipelineRevisionJSON(
                        name: "identity",
                        version: 4,
                        flows: ["discover"],
                        responsePipelineName: "omega"
                    ),
                ])
            ),
            "/admin/v1/pipelines/layout/revisions": .json(
                200,
                Self.pipelineRevisionsJSON(
                    name: "layout",
                    version: 2,
                    flows: ["discover", "load"]
                )
            ),
            "/admin/v1/pipelines/layout/revisions/2/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "layout-discover-v2",
                    x: 20
                )
            ),
            "/admin/v1/pipelines/layout/revisions/2/layout?flow=load": .json(
                503,
                #"{"detail":"load layout temporarily unavailable"}"#
            ),
            "/admin/v1/pipelines/omega/revisions": .json(
                200,
                Self.pipelineRevisionsJSON(
                    name: "omega",
                    version: 3,
                    flows: ["discover"]
                )
            ),
            "/admin/v1/pipelines/omega/revisions/3/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "omega-layout-v3",
                    x: 30
                )
            ),
        ]
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: responses
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)

        await session.refreshAll()

        #expect(session.pipelines.state == .loaded)
        #expect(session.pipelines.pipelines.map(\.name)
            == ["decode", "headless", "identity", "layout", "omega"])
        #expect(session.pipelines.revisions["decode"]?
            .map(\.reference.version) == [2])
        #expect(session.pipelines.layout(
            pipeline: "decode",
            version: 2,
            flow: "discover"
        )?.etag == "decode-layout-v2")
        #expect(session.pipelines.revisions["identity"]?.isEmpty == true)
        #expect(session.pipelines.revisions["headless"]?
            .map(\.reference.version) == [4])
        #expect(session.pipelines.revisions["layout"]?
            .map(\.reference.version) == [2])
        #expect(session.pipelines.layout(
            pipeline: "layout",
            version: 2,
            flow: "discover"
        ) == nil)
        #expect(session.pipelines.layout(
            pipeline: "layout",
            version: 2,
            flow: "load"
        ) == nil)
        #expect(session.pipelines.revisions["omega"]?
            .map(\.reference.version) == [3])
        #expect(session.pipelines.layout(
            pipeline: "omega",
            version: 3,
            flow: "discover"
        )?.etag == "omega-layout-v3")

        let decodeDiagnostic = try #require(
            session.pipelines.diagnostic(for: "decode")
        )
        #expect(Set(decodeDiagnostic.failures.keys) == [.revision(1)])
        #expect(!decodeDiagnostic.usingLastKnownGood)
        let headDiagnostic = try #require(
            session.pipelines.diagnostic(for: "headless")
        )
        #expect(Set(headDiagnostic.failures.keys) == [.revisions])
        #expect(headDiagnostic.message.contains("omitted head v5"))
        #expect(session.pipelines.snapshot(for: "headless")?.isComplete == false)
        let identityDiagnostic = try #require(
            session.pipelines.diagnostic(for: "identity")
        )
        #expect(Set(identityDiagnostic.failures.keys) == [.revision(4)])
        #expect(identityDiagnostic.message.contains("did not match requested"))
        let layoutDiagnostic = try #require(
            session.pipelines.diagnostic(for: "layout")
        )
        #expect(Set(layoutDiagnostic.failures.keys) == [
            .layout(version: 2, flow: "discover"),
            .layout(version: 2, flow: "load"),
        ])
        #expect(!layoutDiagnostic.usingLastKnownGood)
        #expect(session.pipelines.snapshot(for: "layout")?.isComplete == false)
        #expect(session.pipelines.diagnostic(for: "omega") == nil)
        #expect(layoutDiagnostic.message.contains("v2/load"))

        let paths = ControlReconciliationURLProtocol.requests(host: host)
            .map(\.path)
        #expect(paths.contains("/admin/v1/pipelines/omega/revisions"))
        #expect(!paths.contains(
            "/admin/v1/pipelines/omega/revisions/4/layout"
        ))
        #expect(paths.filter {
            $0 == "/admin/v1/pipelines/layout/revisions/2/layout"
        }.count == 2)

        // Loading only the originally failed Flow must not clear diagnostics
        // while its successfully fetched-but-withheld sibling is still absent.
        var oneFlowResponses = responses
        oneFlowResponses[
            "/admin/v1/pipelines/layout/revisions/2/layout?flow=load"
        ] = .json(
            200,
            Self.pipelineLayoutJSON(
                flow: "load",
                etag: "layout-load-v2",
                x: 40
            )
        )
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: oneFlowResponses
        )
        await session.loadLayout(
            pipeline: "layout",
            version: 2,
            flow: "load"
        )
        #expect(session.pipelines.layout(
            pipeline: "layout",
            version: 2,
            flow: "load"
        )?.etag == "layout-load-v2")
        #expect(session.pipelines.layout(
            pipeline: "layout",
            version: 2,
            flow: "discover"
        ) == nil)
        let oneFlowDiagnostic = try #require(
            session.pipelines.diagnostic(for: "layout")
        )
        #expect(Set(oneFlowDiagnostic.failures.keys) == [
            .layout(version: 2, flow: "discover"),
        ])

        // Repeating the same partial failure must not promote the incomplete
        // zero-layout revision to last-known-good.
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: responses
        )
        await session.refreshAll()
        let repeatedDiagnostic = try #require(
            session.pipelines.diagnostic(for: "layout")
        )
        #expect(Set(repeatedDiagnostic.failures.keys) == [
            .layout(version: 2, flow: "discover"),
            .layout(version: 2, flow: "load"),
        ])
        #expect(!repeatedDiagnostic.usingLastKnownGood)
        #expect(session.pipelines.snapshot(for: "layout")?.isComplete == false)

        // An outer revision-list failure preserves every prior scoped failure
        // and still refuses to label the incomplete snapshot as LKG.
        var listFailureResponses = responses
        listFailureResponses["/admin/v1/pipelines/layout/revisions"] = .json(
            503,
            #"{"detail":"revision list temporarily unavailable"}"#
        )
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: listFailureResponses
        )
        await session.refreshAll()
        let listDiagnostic = try #require(
            session.pipelines.diagnostic(for: "layout")
        )
        #expect(Set(listDiagnostic.failures.keys) == [
            .revisions,
            .layout(version: 2, flow: "discover"),
            .layout(version: 2, flow: "load"),
        ])
        #expect(!listDiagnostic.usingLastKnownGood)
        #expect(session.pipelines.snapshot(for: "layout")?.isComplete == false)

        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: oneFlowResponses
        )
        await session.loadLayout(
            pipeline: "layout",
            version: 2,
            flow: "load"
        )
        await session.loadLayout(
            pipeline: "layout",
            version: 2,
            flow: "discover"
        )
        #expect(session.pipelines.snapshot(for: "layout")?.isComplete == true)
        let directRecoveryDiagnostic = try #require(
            session.pipelines.diagnostic(for: "layout")
        )
        #expect(Set(directRecoveryDiagnostic.failures.keys) == [.revisions])

        // The physically complete recovered snapshot is now valid LKG even
        // though the revision-list request itself remains degraded.
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: listFailureResponses
        )
        await session.refreshAll()
        let recoveredLKGDiagnostic = try #require(
            session.pipelines.diagnostic(for: "layout")
        )
        #expect(Set(recoveredLKGDiagnostic.failures.keys) == [.revisions])
        #expect(recoveredLKGDiagnostic.usingLastKnownGood)
        #expect(session.pipelines.snapshot(for: "layout")?.isComplete == true)

        responses[
            "/admin/v1/pipelines/layout/revisions/2/layout?flow=load"
        ] = .json(
            200,
            Self.pipelineLayoutJSON(
                flow: "load",
                etag: "layout-load-v2",
                x: 40
            )
        )
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: responses
        )
        await session.refreshAll()

        #expect(session.pipelines.diagnostic(for: "layout") == nil)
        #expect(session.pipelines.snapshot(for: "layout")?.isComplete == true)
        #expect(session.pipelines.layout(
            pipeline: "layout",
            version: 2,
            flow: "discover"
        )?.etag == "layout-discover-v2")
        #expect(session.pipelines.layout(
            pipeline: "layout",
            version: 2,
            flow: "load"
        )?.etag == "layout-load-v2")
        #expect(session.pipelines.diagnostic(for: "decode") != nil)
        #expect(session.pipelines.diagnostic(for: "identity") != nil)
        #expect(session.pipelines.revisions["omega"]?
            .map(\.reference.version) == [3])
    }

    @Test("a failed Pipeline revision list retains and recovers its exact snapshot")
    func pipelineRefreshRetainsAndRecoversCoherentSnapshot() async throws {
        let host = "pipeline-recovery.windex.test"
        let failedResponses: [String: StubbedControlResponse] = [
            "/admin/v1/pipelines": .json(
                200,
                Self.pipelineListJSON([
                    ("alpha", "Alpha new", 2),
                    ("omega", "Omega", 3),
                ])
            ),
            "/admin/v1/pipelines/alpha/revisions": .json(
                503,
                #"{"detail":"revision list temporarily unavailable"}"#
            ),
            "/admin/v1/pipelines/omega/revisions": .json(
                200,
                Self.pipelineRevisionsJSON(
                    name: "omega",
                    version: 3,
                    flows: ["discover"]
                )
            ),
            "/admin/v1/pipelines/omega/revisions/3/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "omega-layout-v3",
                    x: 30
                )
            ),
        ]
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: failedResponses
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)
        let oldRevision = PipelineRevision(
            reference: .init(
                pipeline: "alpha",
                version: 1,
                specHash: "alpha-hash-1"
            ),
            spec: .init(
                title: "Alpha old",
                flows: [.init(name: "discover")]
            ),
            registryVersion: 1
        )
        let oldLayout = PipelineFlowLayout(
            pipeline: "alpha",
            version: 1,
            flow: "discover",
            positions: ["node": .init(x: 1, y: 1)],
            etag: "alpha-layout-v1"
        )
        session.pipelines.replaceSnapshots(
            [
                .init(
                    summary: .init(
                        name: "alpha",
                        title: "Alpha old",
                        headVersion: 1,
                        headHash: "alpha-hash-1",
                        deploymentCount: 0
                    ),
                    revisions: [oldRevision],
                    layouts: [oldLayout]
                ),
            ],
            diagnostics: [:]
        )

        await session.refreshAll()

        let retained = try #require(
            session.pipelines.pipelines.first { $0.name == "alpha" }
        )
        #expect(retained.title == "Alpha old")
        #expect(retained.headVersion == 1)
        #expect(session.pipelines.revisions["alpha"]?
            .map(\.reference.version) == [1])
        #expect(session.pipelines.layout(
            pipeline: "alpha",
            version: 1,
            flow: "discover"
        )?.etag == "alpha-layout-v1")
        #expect(session.pipelines.revisions["omega"]?
            .map(\.reference.version) == [3])
        let retainedDiagnostic = try #require(
            session.pipelines.diagnostic(for: "alpha")
        )
        #expect(Set(retainedDiagnostic.failures.keys) == [.revisions])
        #expect(retainedDiagnostic.usingLastKnownGood)

        let recoveredResponses: [String: StubbedControlResponse] = [
            "/admin/v1/pipelines": .json(
                200,
                Self.pipelineListJSON([
                    ("alpha", "Alpha new", 2),
                    ("omega", "Omega", 3),
                ])
            ),
            "/admin/v1/pipelines/alpha/revisions": .json(
                200,
                Self.pipelineRevisionsJSON(
                    name: "alpha",
                    version: 2,
                    flows: ["discover"]
                )
            ),
            "/admin/v1/pipelines/alpha/revisions/2/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "alpha-layout-v2",
                    x: 20
                )
            ),
            "/admin/v1/pipelines/omega/revisions": .json(
                200,
                Self.pipelineRevisionsJSON(
                    name: "omega",
                    version: 3,
                    flows: ["discover"]
                )
            ),
            "/admin/v1/pipelines/omega/revisions/3/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "omega-layout-v3",
                    x: 30
                )
            ),
        ]
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: recoveredResponses
        )

        await session.refreshAll()

        let recovered = try #require(
            session.pipelines.pipelines.first { $0.name == "alpha" }
        )
        #expect(recovered.title == "Alpha new")
        #expect(recovered.headVersion == 2)
        #expect(session.pipelines.revisions["alpha"]?
            .map(\.reference.version) == [2])
        #expect(session.pipelines.layout(
            pipeline: "alpha",
            version: 1,
            flow: "discover"
        ) == nil)
        #expect(session.pipelines.layout(
            pipeline: "alpha",
            version: 2,
            flow: "discover"
        )?.etag == "alpha-layout-v2")
        #expect(session.pipelines.diagnostic(for: "alpha") == nil)
        #expect(session.pipelines.state == .loaded)
    }

    @Test("targeted Pipeline failures retain exact LKG and do not stop later targets")
    func targetedPipelineRefreshIsolatesAndRecovers() async throws {
        let host = "pipeline-targeted-isolation.windex.test"
        var responses: [String: StubbedControlResponse] = [
            "/admin/v1/pipelines/broken": .json(
                200,
                Self.pipelineModelJSON(
                    name: "broken",
                    title: "Broken",
                    version: 2
                )
            ),
            "/admin/v1/pipelines/broken/revisions/2": .json(
                200,
                Self.pipelineRevisionJSON(
                    name: "broken",
                    version: 2,
                    flows: ["discover", "load"]
                )
            ),
            "/admin/v1/pipelines/broken/revisions/2/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "broken-discover-new",
                    x: 20
                )
            ),
            "/admin/v1/pipelines/broken/revisions/2/layout?flow=load": .json(
                503,
                #"{"detail":"load layout temporarily unavailable"}"#
            ),
            "/admin/v1/pipelines/omega": .json(
                200,
                Self.pipelineModelJSON(
                    name: "omega",
                    title: "Omega",
                    version: 3
                )
            ),
            "/admin/v1/pipelines/omega/revisions/3": .json(
                200,
                Self.pipelineRevisionJSON(
                    name: "omega",
                    version: 3,
                    flows: ["discover"]
                )
            ),
            "/admin/v1/pipelines/omega/revisions/3/layout?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "omega-discover-v3",
                    x: 30
                )
            ),
        ]
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: responses
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)
        let oldRevision = PipelineRevision(
            reference: .init(
                pipeline: "broken",
                version: 2,
                specHash: "broken-hash-2"
            ),
            spec: .init(
                title: "Broken",
                flows: [
                    .init(name: "discover"),
                    .init(name: "load"),
                ]
            ),
            registryVersion: 1
        )
        session.pipelines.replaceSnapshots(
            [
                .init(
                    summary: .init(
                        name: "broken",
                        title: "Broken",
                        headVersion: 2,
                        headHash: "broken-hash-2"
                    ),
                    revisions: [oldRevision],
                    layouts: [
                        .init(
                            pipeline: "broken",
                            version: 2,
                            flow: "discover",
                            etag: "broken-discover-old"
                        ),
                        .init(
                            pipeline: "broken",
                            version: 2,
                            flow: "load",
                            etag: "broken-load-old"
                        ),
                        .init(
                            pipeline: "broken",
                            version: 2,
                            flow: "obsolete",
                            etag: "broken-obsolete"
                        ),
                    ]
                ),
            ],
            diagnostics: [:]
        )

        let targets: [(Int64, String, Int)] = [
            (1, "broken", 2),
            (2, "omega", 3),
        ]
        for (sequence, pipeline, version) in targets {
            session.scheduleReconciliation(for: .init(
                sequence: sequence,
                timestamp: .now,
                level: .info,
                component: "pipeline",
                pipelineName: pipeline,
                pipelineVersion: version,
                event: "pipeline.revision_published",
                message: ""
            ))
        }
        await session.waitForScheduledReconciliation()

        #expect(session.pipelines.state == .loaded)
        #expect(session.pipelines.layout(
            pipeline: "broken",
            version: 2,
            flow: "discover"
        )?.etag == "broken-discover-old")
        #expect(session.pipelines.layout(
            pipeline: "broken",
            version: 2,
            flow: "load"
        )?.etag == "broken-load-old")
        #expect(session.pipelines.layout(
            pipeline: "broken",
            version: 2,
            flow: "obsolete"
        )?.etag == "broken-obsolete")
        let diagnostic = try #require(
            session.pipelines.diagnostic(for: "broken")
        )
        #expect(Set(diagnostic.failures.keys) == [
            .layout(version: 2, flow: "load"),
        ])
        #expect(diagnostic.usingLastKnownGood)
        #expect(session.pipelines.revisions["omega"]?
            .map(\.reference.version) == [3])
        #expect(session.pipelines.layout(
            pipeline: "omega",
            version: 3,
            flow: "discover"
        )?.etag == "omega-discover-v3")
        #expect(session.pipelines.snapshot(for: "omega")?.isComplete == false)

        responses[
            "/admin/v1/pipelines/broken/revisions/2/layout?flow=load"
        ] = .json(
            200,
            Self.pipelineLayoutJSON(
                flow: "load",
                etag: "broken-load-new",
                x: 40
            )
        )
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: responses
        )
        session.scheduleReconciliation(for: .init(
            sequence: 3,
            timestamp: .now,
            level: .info,
            component: "pipeline",
            pipelineName: "broken",
            pipelineVersion: 2,
            event: "pipeline.layout_updated",
            message: ""
        ))
        await session.waitForScheduledReconciliation()

        #expect(session.pipelines.diagnostic(for: "broken") == nil)
        #expect(session.pipelines.layout(
            pipeline: "broken",
            version: 2,
            flow: "discover"
        )?.etag == "broken-discover-new")
        #expect(session.pipelines.layout(
            pipeline: "broken",
            version: 2,
            flow: "load"
        )?.etag == "broken-load-new")
        #expect(session.pipelines.layout(
            pipeline: "broken",
            version: 2,
            flow: "obsolete"
        ) == nil)
        #expect(session.pipelines.revisions["omega"]?
            .map(\.reference.version) == [3])

        // An exact targeted fetch does not prove complete revision-list
        // membership. A later list failure retains the exact data but must not
        // label the aggregate snapshot as complete LKG.
        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: [
                "/admin/v1/pipelines": .json(
                    200,
                    Self.pipelineListJSON([
                        ("omega", "Omega", 3),
                    ])
                ),
                "/admin/v1/pipelines/omega/revisions": .json(
                    503,
                    #"{"detail":"revision list temporarily unavailable"}"#
                ),
            ]
        )
        await session.refreshAll()

        let omegaDiagnostic = try #require(
            session.pipelines.diagnostic(for: "omega")
        )
        #expect(Set(omegaDiagnostic.failures.keys) == [.revisions])
        #expect(!omegaDiagnostic.usingLastKnownGood)
        #expect(session.pipelines.snapshot(for: "omega")?.isComplete == false)
        #expect(session.pipelines.revisions["omega"]?
            .map(\.reference.version) == [3])
        #expect(session.pipelines.layout(
            pipeline: "omega",
            version: 3,
            flow: "discover"
        )?.etag == "omega-discover-v3")
    }

    @Test("a stopped session ignores a late direct Pipeline layout response")
    func directPipelineLayoutHonorsLifecycleEpoch() async throws {
        let host = "pipeline-layout-lifecycle.windex.test"
        let path = "/admin/v1/pipelines/life/revisions/1/layout"
        let responses: [String: StubbedControlResponse] = [
            "\(path)?flow=discover": .json(
                200,
                Self.pipelineLayoutJSON(
                    flow: "discover",
                    etag: "life-layout-v1",
                    x: 10
                )
            ),
        ]
        ControlReconciliationURLProtocol.configure(
            host: host,
            blocking: [path: 1],
            responses: responses
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)
        let revision = PipelineRevision(
            reference: .init(
                pipeline: "life",
                version: 1,
                specHash: "life-hash-1"
            ),
            spec: .init(
                title: "Life",
                flows: [.init(name: "discover")]
            ),
            registryVersion: 1
        )
        session.pipelines.replaceSnapshots(
            [
                .init(
                    summary: .init(
                        name: "life",
                        title: "Life",
                        headVersion: 1,
                        headHash: "life-hash-1"
                    ),
                    revisions: [revision],
                    layouts: []
                ),
            ],
            diagnostics: [:]
        )

        let stale = Task {
            await session.loadLayout(
                pipeline: "life",
                version: 1,
                flow: "discover"
            )
        }
        #expect(await ControlReconciliationURLProtocol.waitForRequest(
            host: host,
            path: path
        ))
        session.stop()
        ControlReconciliationURLProtocol.release(host: host, path: path)
        await stale.value

        #expect(session.pipelines.layout(
            pipeline: "life",
            version: 1,
            flow: "discover"
        ) == nil)
        #expect(session.pipelines.diagnostic(for: "life") == nil)

        ControlReconciliationURLProtocol.configure(
            host: host,
            responses: responses
        )
        await session.loadLayout(
            pipeline: "life",
            version: 1,
            flow: "discover"
        )
        #expect(session.pipelines.layout(
            pipeline: "life",
            version: 1,
            flow: "discover"
        )?.etag == "life-layout-v1")
        #expect(session.pipelines.snapshot(for: "life")?.isComplete == true)
    }

    @Test("an invalidation arriving during a pass is drained by a second pass")
    func reconciliationDrainsEventsArrivingDuringPass() async throws {
        let host = "reconciliation-rerun.windex.test"
        ControlReconciliationURLProtocol.configure(
            host: host,
            blocking: ["/admin/v1/overview": 1]
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)
        Self.seedDocsSource(in: session)
        let event = Self.runtimeEvent(sequence: 1)

        session.scheduleReconciliation(for: event)
        #expect(await ControlReconciliationURLProtocol.waitForRequest(
            host: host,
            path: "/admin/v1/sources/docs/status"
        ))
        #expect(await ControlReconciliationURLProtocol.waitForRequest(
            host: host,
            path: "/admin/v1/overview"
        ))

        // The Source scope is already loaded, but the first pass has not ended.
        session.scheduleReconciliation(for: Self.runtimeEvent(sequence: 2))
        ControlReconciliationURLProtocol.release(
            host: host,
            path: "/admin/v1/overview"
        )
        await session.waitForScheduledReconciliation()

        let counts = Dictionary(
            grouping: ControlReconciliationURLProtocol.requests(host: host).map(\.path),
            by: { $0 }
        ).mapValues(\.count)
        #expect(counts["/admin/v1/runs/42"] == 2)
        #expect(counts["/admin/v1/sources/docs/status"] == 2)
        #expect(counts["/admin/v1/overview"] == 2)
        #expect(session.runs.runs.first?.id == 42)
        #expect(session.sources.sources.first?.status.currentRun?.id == 42)
        #expect(session.overview.snapshot?.revision == 7)
    }

    @Test("concurrent full refresh requests coalesce before one pass")
    func concurrentFullRefreshesCoalesce() async throws {
        let host = "reconciliation-full-coalesce.windex.test"
        ControlReconciliationURLProtocol.configure(host: host)
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)

        async let first: Void = session.refreshAll()
        async let second: Void = session.refreshAll()
        _ = await (first, second)

        let counts = Dictionary(
            grouping: ControlReconciliationURLProtocol.requests(host: host).map(\.path),
            by: { $0 }
        ).mapValues(\.count)
        for path in [
            "/admin/v1/runs",
            "/admin/v1/sources",
            "/admin/v1/pipelines",
            "/admin/v1/module-health",
            "/admin/v1/log-events",
            "/admin/v1/log-events/facets",
            "/admin/v1/overview",
        ] {
            #expect(counts[path] == 1)
        }
        #expect(session.runs.runs.isEmpty)
        #expect(session.sources.sources.isEmpty)
        #expect(session.overview.snapshot?.revision == 7)
    }

    @Test("a full refresh dominates targeted work in the same batch")
    func fullRefreshDominatesTargetedReconciliation() async throws {
        let host = "reconciliation-full-dominates.windex.test"
        ControlReconciliationURLProtocol.configure(host: host)
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)
        Self.seedDocsSource(in: session)

        session.scheduleReconciliation(for: Self.runtimeEvent(sequence: 1))
        await session.refreshAll()

        let paths = ControlReconciliationURLProtocol.requests(host: host).map(\.path)
        #expect(paths.filter { $0 == "/admin/v1/runs" }.count == 1)
        #expect(paths.filter { $0 == "/admin/v1/sources" }.count == 1)
        #expect(!paths.contains("/admin/v1/runs/42"))
        #expect(!paths.contains("/admin/v1/sources/docs/status"))
        #expect(session.sources.sources.isEmpty)
    }

    @Test("an event storm queues one bounded follow-up pass")
    func reconciliationStormDrainsToStableState() async throws {
        let host = "reconciliation-storm.windex.test"
        ControlReconciliationURLProtocol.configure(
            host: host,
            blocking: ["/admin/v1/overview": 1]
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)
        Self.seedDocsSource(in: session)

        session.scheduleReconciliation(for: Self.runtimeEvent(sequence: 1))
        #expect(await ControlReconciliationURLProtocol.waitForRequest(
            host: host,
            path: "/admin/v1/overview"
        ))
        for sequence in 2...101 {
            session.scheduleReconciliation(
                for: Self.runtimeEvent(sequence: sequence)
            )
        }
        ControlReconciliationURLProtocol.release(
            host: host,
            path: "/admin/v1/overview"
        )
        await session.waitForScheduledReconciliation()

        let counts = Dictionary(
            grouping: ControlReconciliationURLProtocol.requests(host: host).map(\.path),
            by: { $0 }
        ).mapValues(\.count)
        #expect(counts["/admin/v1/runs/42"] == 2)
        #expect(counts["/admin/v1/sources/docs/status"] == 2)
        #expect(counts["/admin/v1/overview"] == 2)
        #expect(ControlReconciliationURLProtocol.requests(host: host).count == 6)
        #expect(session.sources.sources.first?.status.activity == .running)
    }

    @Test("stopping cancels stale reconciliation without clearing restarted work")
    func stopCancelsReconciliationGeneration() async throws {
        let host = "reconciliation-stop.windex.test"
        ControlReconciliationURLProtocol.configure(
            host: host,
            blocking: ["/admin/v1/runs/42": 1]
        )
        defer { ControlReconciliationURLProtocol.reset(host: host) }
        let session = try Self.controlSession(host: host)
        Self.seedDocsSource(in: session)

        session.scheduleReconciliation(for: Self.runtimeEvent(sequence: 1))
        #expect(await ControlReconciliationURLProtocol.waitForRequest(
            host: host,
            path: "/admin/v1/runs/42"
        ))
        session.stop()
        ControlReconciliationURLProtocol.release(
            host: host,
            path: "/admin/v1/runs/42"
        )

        // A fresh generation must not be cleared by the cancelled driver's
        // eventual return, even when the transport ignores cancellation.
        session.scheduleReconciliation(for: Self.runtimeEvent(sequence: 2))
        await session.waitForScheduledReconciliation()

        let paths = ControlReconciliationURLProtocol.requests(host: host).map(\.path)
        #expect(paths.filter { $0 == "/admin/v1/runs/42" }.count == 2)
        #expect(paths.filter { $0 == "/admin/v1/sources/docs/status" }.count == 1)
        #expect(paths.filter { $0 == "/admin/v1/overview" }.count == 1)
        #expect(session.runs.runs.first?.id == 42)
        #expect(session.sources.sources.first?.status.currentRun?.id == 42)
    }

    private static let evidence = PairingEvidence(
        version: "0.1.0",
        uptimeSeconds: 128,
        authRequired: true,
        scopes: ["admin"])

    private static func controlSession(host: String) throws -> BackendSession {
        let profile = try ConnectionProfile("http://\(host)")
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [ControlReconciliationURLProtocol.self]
        let client = WindexClient(
            configuration: .init(baseURL: profile.baseURL),
            token: "token",
            session: URLSession(configuration: configuration)
        )
        return BackendSession(
            client: client,
            backend: ConnectedBackend(
                profile: profile,
                evidence: evidence,
                hasStoredToken: true
            ),
            reconciliationDelay: .milliseconds(10)
        )
    }

    private static func seedDocsSource(in session: BackendSession) {
        session.runs.replace([])
        session.sources.replace([
            SourceDeployment(
                name: "docs",
                title: "Docs",
                origin: "push",
                pipeline: .init(
                    pipeline: "crawl",
                    version: 2,
                    specHash: "hash-2"
                ),
                search: .init(
                    searchName: "docs",
                    idPrefix: "docs:",
                    collectionKey: "docs",
                    searchProfile: "generic",
                    includeInAll: true
                ),
                stateNamespace: "docs"
            ),
        ])
    }

    private static func runtimeEvent(sequence: Int) -> OperationalEvent {
        OperationalEvent(
            sequence: sequence,
            timestamp: .now,
            level: .info,
            component: "worker",
            sourceName: "docs",
            pipelineName: "crawl",
            pipelineVersion: 3,
            runID: 42,
            taskID: 7,
            event: "task.leased",
            message: ""
        )
    }

    private static func pipelineListJSON(
        _ pipelines: [(name: String, title: String, version: Int)]
    ) -> String {
        let values = pipelines.enumerated().map { offset, pipeline in
            pipelineModelJSON(
                name: pipeline.name,
                title: pipeline.title,
                version: pipeline.version,
                id: offset + 1
            )
        }.joined(separator: ",")
        return #"{"pipelines":[\#(values)]}"#
    }

    private static func pipelineModelJSON(
        name: String,
        title: String,
        version: Int,
        id: Int = 1,
        responseName: String? = nil
    ) -> String {
        """
        {
          "builtin": true,
          "created_at": "2026-07-25T00:00:00Z",
          "description": "",
          "head_revision_id": \(version),
          "id": \(id),
          "name": "\(responseName ?? name)",
          "spec_hash": "\(name)-hash-\(version)",
          "title": "\(title)",
          "updated_at": "2026-07-26T10:00:00Z",
          "version": \(version)
        }
        """
    }

    private static func pipelineRevisionsJSON(
        name: String,
        version: Int,
        flows: [String],
        validSpec: Bool = true
    ) -> String {
        pipelineRevisionsJSON([
            pipelineRevisionJSON(
                name: name,
                version: version,
                flows: flows,
                validSpec: validSpec
            ),
        ])
    }

    private static func pipelineRevisionsJSON(
        _ revisions: [String]
    ) -> String {
        #"{"revisions":[\#(revisions.joined(separator: ","))]}"#
    }

    private static func pipelineRevisionJSON(
        name: String,
        version: Int,
        flows: [String],
        validSpec: Bool = true,
        responsePipelineName: String? = nil
    ) -> String {
        let flowValue: String
        if validSpec {
            flowValue = flows.map { flow in
                """
                "\(flow)": {
                  "inputs": [],
                  "outputs": [],
                  "nodes": {},
                  "edges": []
                }
                """
            }.joined(separator: ",")
        } else {
            flowValue = ""
        }
        let spec = validSpec
            ? """
              {
                "schema": "windex.pipeline/1",
                "parameters": [],
                "state": {},
                "flows": {\(flowValue)},
                "refresh": []
              }
              """
            : """
              {
                "schema": "windex.pipeline/1",
                "parameters": [],
                "state": {},
                "flows": [],
                "refresh": []
              }
              """
        return """
        {
          "author": "test",
          "capability": {"capable": false, "issues": []},
          "created_at": "2026-07-26T10:00:00Z",
          "id": \(version),
          "module_locks": {},
          "note": "",
          "parent_revision_id": null,
          "pipeline_id": 1,
          "pipeline_name": "\(responsePipelineName ?? name)",
          "registry_digest": "registry",
          "registry_version": 1,
          "spec": \(spec),
          "spec_hash": "\(name)-hash-\(version)",
          "version": \(version)
        }
        """
    }

    private static func pipelineLayoutJSON(
        flow: String,
        etag: String,
        x: Int
    ) -> String {
        """
        {
          "etag": "\(etag)",
          "flow": "\(flow)",
          "layout": {
            "nodes": {"node": {"x": \(x), "y": 0}},
            "groups": [],
            "annotations": []
          },
          "updated_at": "2026-07-26T10:00:00Z"
        }
        """
    }

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

    private static func sourceSettingsScope(
        source: String,
        value: Int
    ) throws -> SettingsScope {
        try JSONDecoder().decode(
            SettingsScope.self,
            from: Data(
                """
                {
                  "scope": "\(source)",
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

    private static func sourceTrigger(
        id: Int,
        hour: Int
    ) throws -> SourceTriggerWire {
        try JSONDecoder().decode(
            SourceTriggerWire.self,
            from: Data(
                """
                {
                  "enabled": true,
                  "flow_name": "harvest",
                  "id": \(id),
                  "last_fired_at": null,
                  "last_run_id": null,
                  "next_fire_at": "2026-07-27T\(hour):00:00Z",
                  "trigger_spec": {"cron": "0 \(hour) * * *"},
                  "trigger_type": "schedule"
                }
                """.utf8
            )
        )
    }

    private static func sourceIsolationResponses(
        memorySettingsStatus: Int
    ) -> [String: StubbedControlResponse] {
        let docs = sourceWire(
            name: "docs",
            title: "Docs",
            pipeline: "crawl",
            batch: 16
        )
        let memory = sourceWire(
            name: "memory",
            title: "Memory",
            pipeline: "memory",
            batch: 20
        )
        var responses: [String: StubbedControlResponse] = [
            "/admin/v1/sources": .json(
                200,
                #"{"sources":[\#(docs),\#(memory)]}"#
            ),
        ]
        for (name, wire, batch) in [
            ("docs", docs, 16),
            ("memory", memory, 20),
        ] {
            responses["/admin/v1/sources/\(name)"] = .json(200, wire)
            responses["/admin/v1/sources/\(name)/runs"] = .json(
                200,
                #"{"runs":[]}"#
            )
            responses["/admin/v1/sources/\(name)/settings"] = .json(
                name == "memory" ? memorySettingsStatus : 200,
                name == "memory" && memorySettingsStatus != 200
                    ? #"{"detail":"memory settings temporarily unavailable"}"#
                    : sourceSettingsWire(
                        source: name,
                        pipeline: name == "docs" ? "crawl" : "memory",
                        batch: batch
                    )
            )
            responses["/admin/v1/sources/\(name)/triggers"] = .json(
                200,
                sourceTriggersWire()
            )
            responses["/admin/v1/sources/\(name)/status"] = .json(
                200,
                sourceStatusWire(source: name)
            )
        }
        return responses
    }

    private static func sourceWire(
        name: String,
        title: String,
        pipeline: String,
        batch: Int
    ) -> String {
        """
        {
          "id": \(name == "docs" ? 1 : 2),
          "name": "\(name)",
          "title": "\(title)",
          "description": "",
          "origin": {"ingress": "push"},
          "pipeline_revision_id": 2,
          "pipeline_name": "\(pipeline)",
          "pipeline_version": 2,
          "pipeline_hash": "\(pipeline)-hash-2",
          "search_contract_version": "1",
          "search_name": "\(name)",
          "id_prefix": "\(name):",
          "collection_key": "\(name)",
          "search_profile": "\(name)",
          "include_in_all": true,
          "state_namespace": "\(name)",
          "enabled": true,
          "generation": 2,
          "archived_at": null,
          "created_at": "2026-07-26T10:00:00Z",
          "updated_at": "2026-07-26T10:00:01Z",
          "values": {"batch": \(batch)},
          "values_hash": "\(name)-settings-v2",
          "paused": false,
          "pause_reason": "",
          "paused_at": null,
          "etag": "\(name)-source-v2",
          "ready": true,
          "ingress": null
        }
        """
    }

    private static func sourceSettingsWire(
        source: String,
        pipeline: String,
        batch: Int
    ) -> String {
        """
        {
          "etag": "\(source)-settings-v2",
          "fields": [{
            "key": "batch",
            "kind": "int",
            "title": "Batch",
            "value": \(batch),
            "origin": "source"
          }],
          "pipeline": "\(pipeline)",
          "pipeline_version": 2,
          "source": "\(source)",
          "values": {"batch": \(batch)}
        }
        """
    }

    private static func sourceTriggersWire() -> String {
        """
        {
          "triggers": [{
            "enabled": true,
            "flow_name": "harvest",
            "id": 22,
            "last_fired_at": null,
            "last_run_id": null,
            "next_fire_at": "2026-07-27T12:00:00Z",
            "trigger_spec": {"cron": "0 12 * * *"},
            "trigger_type": "schedule"
          }]
        }
        """
    }

    private static func sourceStatusWire(source: String) -> String {
        """
        {
          "current_run": null,
          "documents": {},
          "enabled": true,
          "last_failure": null,
          "last_success": null,
          "latest_run": null,
          "paused": false,
          "recent_error": null,
          "source": "\(source)"
        }
        """
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

private struct RecordedControlRequest: Sendable {
    let host: String
    let method: String
    let path: String
}

private struct StubbedControlResponse: Sendable {
    let statusCode: Int
    let body: Data

    static func json(
        _ statusCode: Int,
        _ body: String
    ) -> StubbedControlResponse {
        .init(statusCode: statusCode, body: Data(body.utf8))
    }
}

private final class ControlReconciliationURLProtocol:
    URLProtocol,
    @unchecked Sendable
{
    private static let lock = NSLock()
    nonisolated(unsafe) private static var recorded: [RecordedControlRequest] = []
    nonisolated(unsafe) private static var gates:
        [String: [String: [DispatchSemaphore]]] = [:]
    nonisolated(unsafe) private static var responses:
        [String: [String: StubbedControlResponse]] = [:]

    static func requests(host: String) -> [RecordedControlRequest] {
        lock.withLock { recorded.filter { $0.host == host } }
    }

    static func configure(
        host: String,
        blocking: [String: Int] = [:],
        responses: [String: StubbedControlResponse] = [:]
    ) {
        lock.withLock {
            recorded.removeAll { $0.host == host }
            gates[host] = blocking.mapValues { count in
                (0..<count).map { _ in DispatchSemaphore(value: 0) }
            }
            Self.responses[host] = responses
        }
    }

    static func reset(host: String) {
        let pending = lock.withLock { () -> [DispatchSemaphore] in
            recorded.removeAll { $0.host == host }
            responses.removeValue(forKey: host)
            return gates.removeValue(forKey: host)?
                .values.flatMap { $0 } ?? []
        }
        pending.forEach { $0.signal() }
    }

    static func waitForRequest(
        host: String,
        path: String,
        count: Int = 1
    ) async -> Bool {
        for _ in 0..<2_000 {
            if requests(host: host).filter({ $0.path == path }).count >= count {
                return true
            }
            try? await Task.sleep(for: .milliseconds(1))
        }
        return false
    }

    static func release(host: String, path: String, index: Int = 0) {
        let semaphore = lock.withLock { () -> DispatchSemaphore? in
            guard let values = gates[host]?[path],
                  values.indices.contains(index) else { return nil }
            return values[index]
        }
        semaphore?.signal()
    }

    override class func canInit(with request: URLRequest) -> Bool { true }

    override class func canonicalRequest(
        for request: URLRequest
    ) -> URLRequest {
        request
    }

    override func startLoading() {
        let path = request.url?.path ?? ""
        let responseKey = request.url?.query.map { "\(path)?\($0)" } ?? path
        let configured = Self.lock.withLock {
            () -> (DispatchSemaphore?, StubbedControlResponse?) in
            let host = request.url?.host ?? ""
            let ordinal = Self.recorded.filter {
                $0.host == host && $0.path == path
            }.count
            Self.recorded.append(.init(
                host: host,
                method: request.httpMethod ?? "GET",
                path: path
            ))
            let values = Self.gates[host]?[path]
            let gate = values?.indices.contains(ordinal) == true
                ? values?[ordinal]
                : nil
            return (
                gate,
                Self.responses[host]?[responseKey]
                    ?? Self.responses[host]?[path]
            )
        }
        configured.0?.wait()
        let defaultBody = Self.responseBody(path: path)
        let body: Data?
        let status: Int
        if let response = configured.1 {
            body = response.body
            status = response.statusCode
        } else {
            body = defaultBody
            status = defaultBody == nil ? 404 : 200
        }
        guard let url = request.url,
              let response = HTTPURLResponse(
                url: url,
                statusCode: status,
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
        client?.urlProtocol(
            self,
            didLoad: body ?? Data(#"{"detail":"not configured"}"#.utf8)
        )
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}

    private static func responseBody(path: String) -> Data? {
        let body: String
        switch path {
        case "/admin/v1/runs":
            body = #"{"runs":[]}"#
        case "/admin/v1/sources":
            body = #"{"sources":[]}"#
        case "/admin/v1/pipelines":
            body = #"{"pipelines":[]}"#
        case "/admin/v1/log-events":
            body = #"{"events":[],"next_cursor":0}"#
        case "/admin/v1/log-events/facets":
            body = """
            {
              "levels": [],
              "components": [],
              "sources": [],
              "pipelines": [],
              "nodes": [],
              "modules": []
            }
            """
        case "/admin/v1/runs/42":
            body = """
            {
              "cancel_requested": false,
              "dedupe_key": null,
              "effective_config": {},
              "error": null,
              "explicit_inputs": {},
              "finished_at": null,
              "flow_name": "harvest",
              "id": 42,
              "idempotency_key": null,
              "mode": "normal",
              "module_locks": {},
              "pipeline_hash": "hash-3",
              "pipeline_name": "crawl",
              "pipeline_revision_id": 3,
              "pipeline_version": 3,
              "priority": 50,
              "progress": {"fraction": 0.5},
              "queued_at": "2026-07-26T10:00:00Z",
              "source_id": 1,
              "source_name": "docs",
              "started_at": "2026-07-26T10:00:01Z",
              "state": "running",
              "stats": {},
              "trigger_by": "worker",
              "trigger_type": "manual",
              "updated_at": "2026-07-26T10:00:02Z"
            }
            """
        case "/admin/v1/sources/docs/status":
            body = """
            {
              "current_run": {"id": 42},
              "documents": {},
              "enabled": true,
              "last_failure": null,
              "last_success": null,
              "latest_run": {"id": 42},
              "paused": false,
              "recent_error": null,
              "source": "docs"
            }
            """
        case "/admin/v1/sources/newdocs":
            body = """
            {
              "id": 2,
              "name": "newdocs",
              "title": "New Docs",
              "description": "",
              "origin": {"ingress": "push"},
              "pipeline_revision_id": 3,
              "pipeline_name": "crawl",
              "pipeline_version": 3,
              "pipeline_hash": "hash-3",
              "search_contract_version": "1",
              "search_name": "newdocs",
              "id_prefix": "newdocs:",
              "collection_key": "newdocs",
              "search_profile": "generic",
              "include_in_all": true,
              "state_namespace": "newdocs",
              "enabled": true,
              "generation": 2,
              "archived_at": null,
              "created_at": "2026-07-26T10:00:00Z",
              "updated_at": "2026-07-26T10:00:01Z",
              "values": {"batch": 16},
              "values_hash": "settings-v3",
              "paused": false,
              "pause_reason": "",
              "paused_at": null,
              "etag": "source-v3",
              "ready": true,
              "ingress": null
            }
            """
        case "/admin/v1/sources/newdocs/runs":
            body = #"{"runs":[]}"#
        case "/admin/v1/sources/newdocs/settings":
            body = """
            {
              "etag": "settings-v3",
              "fields": [{
                "key": "batch",
                "kind": "int",
                "title": "Batch",
                "value": 16,
                "origin": "source"
              }],
              "pipeline": "crawl",
              "pipeline_version": 3,
              "source": "newdocs",
              "values": {"batch": 16}
            }
            """
        case "/admin/v1/sources/newdocs/triggers":
            body = #"{"triggers":[]}"#
        case "/admin/v1/sources/newdocs/status":
            body = """
            {
              "current_run": null,
              "documents": {},
              "enabled": true,
              "last_failure": null,
              "last_success": null,
              "latest_run": null,
              "paused": false,
              "recent_error": null,
              "source": "newdocs"
            }
            """
        case "/admin/v1/sources/newdocs/module-status":
            body = """
            {
              "available": true,
              "latest_pipeline_version": 3,
              "pipeline_revision_id": 3,
              "pipeline_version": 3,
              "source": "newdocs",
              "unavailable_modules": [],
              "upgrade_required": false
            }
            """
        case "/admin/v1/module-health":
            body = """
            {
              "sources": [{
                "available": true,
                "latest_pipeline_version": 3,
                "pipeline_revision_id": 3,
                "pipeline_version": 3,
                "source": "newdocs",
                "unavailable_modules": [],
                "upgrade_required": false
              }],
              "status": "ok",
              "stranded_sources": 0
            }
            """
        case "/admin/v1/pipelines/crawl":
            body = """
            {
              "id": 1,
              "name": "crawl",
              "title": "Crawl",
              "description": "",
              "builtin": true,
              "archived_at": null,
              "created_at": "2026-07-25T00:00:00Z",
              "updated_at": "2026-07-26T10:00:00Z",
              "head_revision_id": 3,
              "version": 3,
              "spec_hash": "hash-3"
            }
            """
        case "/admin/v1/pipelines/crawl/revisions/3":
            body = """
            {
              "author": "test",
              "capability": {"capable": false, "issues": []},
              "created_at": "2026-07-26T10:00:00Z",
              "id": 3,
              "module_locks": {},
              "note": "",
              "parent_revision_id": 2,
              "pipeline_id": 1,
              "pipeline_name": "crawl",
              "registry_digest": "registry",
              "registry_version": 1,
              "spec": {
                "schema": "windex.pipeline/1",
                "parameters": [],
                "state": {},
                "flows": {},
                "refresh": []
              },
              "spec_hash": "hash-3",
              "version": 3
            }
            """
        case "/admin/v1/overview":
            body = """
            {
              "revision": 7,
              "as_of": "2026-07-26T10:00:03Z",
              "health": {
                "service": "ok",
                "postgres": "ok",
                "vector": "ok",
                "storage": "ok",
                "module_locks": "ok",
                "stranded_sources": [],
                "degraded": false
              },
              "runs": {
                "counts": {
                  "queued": 0,
                  "running": 1,
                  "blocked": 0,
                  "failed": 0,
                  "succeeded": 0,
                  "cancelled": 0
                },
                "active": [],
                "recent": []
              },
              "workers": {"lanes": {}, "blocked_preconditions": []},
              "sources": [{
                "name": "docs",
                "enabled": true,
                "paused": false,
                "documents": 0,
                "searchable": 0,
                "last_indexed_at": null,
                "as_of": "2026-07-26T10:00:03Z"
              }],
              "schedules": [],
              "recent_documents": [],
              "totals": {
                "documents": 0,
                "searchable": 0,
                "vectors": 0,
                "indexed_last_hour": 0,
                "as_of": "2026-07-26T10:00:03Z"
              }
            }
            """
        default:
            return nil
        }
        return Data(body.utf8)
    }
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
