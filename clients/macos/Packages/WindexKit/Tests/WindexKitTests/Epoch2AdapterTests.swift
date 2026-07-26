import Foundation
import Testing
@testable import WindexKit

@Suite("Epoch-2 canonical adapters")
struct Epoch2AdapterTests {
    @Test(
        "Overview preserves every Module-lock health state",
        arguments: [
            ("ok", OverviewModuleLockHealth.ok),
            ("degraded", OverviewModuleLockHealth.degraded),
            ("error", OverviewModuleLockHealth.error),
        ]
    )
    func moduleLockHealthStates(
        raw: String,
        expected: OverviewModuleLockHealth
    ) {
        #expect(OverviewModuleLockHealth(rawValue: raw) == expected)
    }

    @Test("canonical layout arrays survive an exact GET/edit/PUT round trip")
    func layoutNodes() throws {
        let wire = try JSONDecoder().decode(
            PipelineLayoutWire.self,
            from: Data(
                #"""
                {
                  "flow": "main",
                  "layout": {
                    "nodes": {"fetch": {"x": 12, "y": 34}},
                    "groups": [{
                      "id": "ingress",
                      "title": "Ingress",
                      "nodes": ["fetch"],
                      "x": 4,
                      "y": 8,
                      "width": 300,
                      "height": 160,
                      "color": "cyan"
                    }],
                    "annotations": [{
                      "id": "note-1",
                      "text": "Starts here",
                      "x": 18,
                      "y": 22,
                      "width": 210,
                      "height": 72,
                      "pinned": true
                    }]
                  },
                  "etag": "layout-2",
                  "updated_at": "2026-07-25T12:00:00Z"
                }
                """#.utf8
            )
        )
        var layout = try wire.flowLayout(pipeline: "docs", version: 3)
        #expect(layout.positions["fetch"] == .init(x: 12, y: 34))
        #expect(layout.groups.count == 1)
        #expect(layout.groups[0].nodes == ["fetch"])
        #expect(layout.groups[0].fields["color"] == .string("cyan"))
        #expect(layout.annotations.count == 1)
        #expect(layout.annotations[0].fields["pinned"] == .bool(true))
        #expect(layout.etag == "layout-2")
        #expect(layout.updatedAt == "2026-07-25T12:00:00Z")

        layout.positions["fetch"] = .init(x: 90, y: 120)
        let payload = try layout.wirePayload()
        #expect(payload["nodes"]?.objectValue?["fetch"]?.objectValue?["x"] == .double(90))
        #expect(payload["groups"]?.arrayValue?.count == 1)
        #expect(payload["groups"]?.arrayValue?.first?.objectValue?["nodes"]?.arrayValue?
            .compactMap(\.stringValue) == ["fetch"])
        #expect(payload["groups"]?.arrayValue?.first?.objectValue?["color"] == .string("cyan"))
        #expect(payload["annotations"]?.arrayValue?.count == 1)
        #expect(payload["annotations"]?.arrayValue?.first?.objectValue?["pinned"] == .bool(true))
    }

    @Test(
        "Source status projects queued, running, blocked, failed, paused, and idle",
        arguments: [
            ("queued", SourceActivityState.queued, false, false),
            ("running", SourceActivityState.running, false, false),
            ("blocked", SourceActivityState.blocked, false, false),
            ("failed", SourceActivityState.failed, false, true),
            ("running", SourceActivityState.paused, true, false),
            ("idle", SourceActivityState.idle, false, false),
        ]
    )
    func sourceStates(
        currentState: String,
        expected: SourceActivityState,
        paused: Bool,
        recentFailure: Bool
    ) throws {
        let hasCurrent = currentState != "idle"
        let wire = try JSONDecoder().decode(
            SourceStatusWire.self,
            from: Data(
                """
                {
                  "source": "docs",
                  "enabled": true,
                  "paused": \(paused),
                  "latest_run": \(hasCurrent ? #"{"id":7}"# : "null"),
                  "current_run": \(hasCurrent ? #"{"id":7}"# : "null"),
                  "documents": {},
                  "last_success": null,
                  "last_failure": \(recentFailure ? #""2026-07-25T12:00:00Z""# : "null"),
                  "recent_error": \(recentFailure ? #""boom""# : "null")
                }
                """.utf8
            )
        )
        let runs = hasCurrent ? [
            SourceRunSummary(
                id: 7,
                sourceName: "docs",
                pipeline: .init(pipeline: "push", version: 2, specHash: "hash"),
                state: SourceActivityState(rawValue: currentState) ?? .idle,
                flow: "main"
            ),
        ] : []
        #expect(try wire.status(runs: runs).activity == expected)
    }

    @Test("Overview maps only canonical epoch-2 keys")
    func overviewProjection() throws {
        let wire = try JSONDecoder().decode(
            OverviewWire.self,
            from: Data(
                #"""
                {
                  "revision": 42,
                  "as_of": "2026-07-25T12:00:00Z",
                  "health": {
                    "service": "ok",
                    "postgres": "ok",
                    "vector": "ok",
                    "storage": "ok",
                    "module_locks": "degraded",
                    "stranded_sources": ["docs"],
                    "degraded": false
                  },
                  "runs": {
                    "counts": {
                      "queued": 2,
                      "running": 1,
                      "blocked": 3,
                      "failed": 4,
                      "succeeded": 5,
                      "cancelled": 6
                    },
                    "active": [{
                      "id": 9,
                      "source_name": "docs",
                      "pipeline_name": "push",
                      "pipeline_version": 3,
                      "flow_name": "receive",
                      "state": "running",
                      "progress": {"fraction": 0.5}
                    }],
                    "recent": []
                  },
                  "workers": {
                    "lanes": {"io": {"ready": 2, "running": 1}},
                    "blocked_preconditions": [{
                      "preconditions": ["gpu"],
                      "reason": "capacity",
                      "tasks": 3
                    }]
                  },
                  "sources": [{
                    "name": "docs",
                    "enabled": true,
                    "paused": false,
                    "documents": 100,
                    "searchable": 90,
                    "last_indexed_at": "2026-07-25T11:59:00Z",
                    "as_of": "2026-07-25T12:00:00Z"
                  }],
                  "schedules": [{
                    "source": "docs",
                    "next_trigger": "2026-07-25T13:00:00Z"
                  }],
                  "recent_documents": [{
                    "id": "doc-1",
                    "source": "docs",
                    "title": "One",
                    "indexed_at": "2026-07-25T11:59:00Z"
                  }],
                  "totals": {
                    "documents": 100,
                    "searchable": 90,
                    "vectors": 88,
                    "indexed_last_hour": 7,
                    "as_of": "2026-07-25T12:00:00Z"
                  }
                }
                """#.utf8
            )
        )
        let source = SourceDeployment(
            name: "docs",
            title: "Docs",
            origin: "push",
            pipeline: .init(pipeline: "push", version: 3, specHash: "hash"),
            search: .init(
                searchName: "docs",
                idPrefix: "docs:",
                collectionKey: "docs",
                searchProfile: "default",
                includeInAll: true
            ),
            stateNamespace: "docs"
        )
        let snapshot = try wire.snapshot(sourceDeployments: [source])

        #expect(snapshot.documents == 100)
        #expect(snapshot.searchable == 90)
        #expect(snapshot.vectors == 88)
        #expect(snapshot.indexedLastHour == 7)
        #expect(snapshot.moduleLocks == .degraded)
        #expect(snapshot.strandedSources == ["docs"])
        #expect(snapshot.runs.running == 1)
        #expect(snapshot.runs.blocked == 3)
        #expect(snapshot.sources.first?.documents == 100)
        #expect(snapshot.sources.first?.nextTrigger == "2026-07-25T13:00:00Z")
        #expect(snapshot.workerLanes.first?.states["ready"] == 2)
        #expect(snapshot.blockedPreconditions.first?.tasks == 3)
        #expect(snapshot.activeRuns.first?.progress == 0.5)
        #expect(snapshot.recentDocuments.first?.id == "doc-1")
    }
}
