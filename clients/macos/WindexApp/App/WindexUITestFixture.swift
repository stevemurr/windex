#if DEBUG
import Foundation
import WindexKit

@MainActor
enum WindexUITestFixture {
    static func session() -> BackendSession {
        let profile = try! ConnectionProfile("http://127.0.0.1:9")
        let backend = ConnectedBackend(
            profile: profile,
            evidence: PairingEvidence(
                version: "ui-test",
                uptimeSeconds: 0,
                authRequired: false,
                scopes: ["admin"],
                contractEpoch: 2
            ),
            hasStoredToken: false
        )
        let session = BackendSession(
            client: WindexClient(baseURL: profile.baseURL),
            backend: backend
        )
        let reference = PipelineRevisionReference(
            pipeline: "arxiv",
            version: 1,
            specHash: "ui-test-arxiv-v1"
        )
        let revision = PipelineRevision(
            reference: reference,
            spec: PipelineSpec(
                title: "arXiv papers",
                flows: [
                    PipelineFlow(
                        name: "harvest",
                        nodes: [
                            PipelineNode(
                                id: "windows",
                                kind: "discover",
                                module: "time.windows"
                            ),
                            PipelineNode(
                                id: "stage",
                                kind: "load",
                                module: "ledger.stage"
                            ),
                        ]
                    ),
                ],
                refreshFlows: ["harvest"]
            ),
            registryVersion: 1,
            sourceCapability: .init(capable: true)
        )
        session.registry.replaceForUITesting(
            PipelineRegistry(
                version: 1,
                portTypes: [],
                kinds: [
                    .init(
                        id: "discover",
                        title: "Discover",
                        description: "",
                        inputType: nil,
                        outputType: nil,
                        stateful: true
                    ),
                    .init(
                        id: "load",
                        title: "Load",
                        description: "",
                        inputType: nil,
                        outputType: nil,
                        stateful: true
                    ),
                ],
                modules: [
                    .init(
                        id: "time.windows",
                        kind: "discover",
                        version: "1",
                        title: "Time windows",
                        contractRoles: ["ingress.pull", "state.read"]
                    ),
                    .init(
                        id: "ledger.stage",
                        kind: "load",
                        version: "1",
                        title: "Stage documents",
                        contractRoles: [
                            "document.identity",
                            "document.provenance",
                            "document.staging",
                        ]
                    ),
                ]
            )
        )
        session.pipelines.replace([
            PipelineSummary(
                name: "arxiv",
                title: "arXiv papers",
                headVersion: 1,
                headHash: reference.specHash,
                builtin: true,
                deploymentCount: 1
            ),
        ])
        session.pipelines.replaceRevisions([revision], for: "arxiv")
        session.sources.replace([
            SourceDeployment(
                name: "arxiv",
                title: "arXiv papers",
                origin: "pull",
                pipeline: reference,
                search: SourceSearchIdentity(
                    searchName: "arxiv",
                    idPrefix: "arxiv:",
                    collectionKey: "arxiv",
                    searchProfile: "arxiv",
                    includeInAll: true
                ),
                stateNamespace: "arxiv"
            ),
        ])
        return session
    }
}
#endif
