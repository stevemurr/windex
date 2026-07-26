import Foundation
import Testing
@testable import WindexKit

@Suite("Epoch-2 canonical adapters")
struct Epoch2AdapterTests {
    @Test("layout nodes round-trip into canvas positions")
    func layoutNodes() throws {
        let wire = try JSONDecoder().decode(
            PipelineLayoutWire.self,
            from: Data(
                #"""
                {
                  "flow": "main",
                  "layout": {
                    "nodes": {"fetch": {"x": 12, "y": 34}},
                    "groups": [],
                    "annotations": []
                  },
                  "etag": "layout-2",
                  "updated_at": "2026-07-25T12:00:00Z"
                }
                """#.utf8
            )
        )
        let layout = try wire.flowLayout(pipeline: "docs", version: 3)
        #expect(layout.positions["fetch"] == .init(x: 12, y: 34))
        #expect(layout.etag == "layout-2")
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
