import Foundation
import Testing
@testable import Windex

@Suite("Overview presentation")
struct OverviewModelTests {
    @Test("stats and freshness become the colophon figures")
    func snapshot() {
        let now = Date(timeIntervalSince1970: 2_000_000)
        let snapshot = ColophonSnapshot(
            embedsPerMinute: 1542.5,
            documentCount: 17_493_416,
            uptimeSeconds: 15_120,
            sources: [
                SourceRow(
                    name: "news",
                    indexed: 2_432_790,
                    pending: 118_204,
                    lastActivity: now.addingTimeInterval(-60),
                    now: now),
                SourceRow(
                    name: "wiki",
                    indexed: 2_106_024,
                    pending: 0,
                    lastActivity: now.addingTimeInterval(-3_600),
                    now: now),
            ])

        #expect(snapshot.embedsPerMinute == 1542.5)
        #expect(snapshot.documentCount == 17_493_416)
        #expect(snapshot.uptimeSeconds == 15_120)
        #expect(snapshot.sources.map(\.name) == ["news", "wiki"])
    }

    @Test("source state uses activity and backlog rather than green ticks")
    func sourceConditions() {
        let now = Date(timeIntervalSince1970: 2_000_000)
        let running = SourceRow(
            name: "arxiv",
            indexed: 10,
            pending: 4,
            lastActivity: now.addingTimeInterval(-30),
            now: now)
        let queued = SourceRow(
            name: "docs",
            indexed: 10,
            pending: 4,
            lastActivity: now.addingTimeInterval(-3_600),
            now: now)
        let healthy = SourceRow(
            name: "wiki",
            indexed: 10,
            pending: 0,
            lastActivity: now.addingTimeInterval(-3_600),
            now: now)

        #expect(running.condition == .running)
        #expect(queued.condition == .attention("queued"))
        #expect(healthy.condition == .healthy)
    }
}
