import Foundation
import Testing
@testable import WindexKit

@Suite("Operational Events")
struct OperationalEventTests {
    @Test("the bounded buffer deduplicates reconnect overlap")
    func deduplicatesAndBounds() {
        var buffer = OperationalEventBuffer(capacity: 3)
        buffer.append([event(1), event(2)])
        buffer.append([event(2), event(3), event(4), event(4)])

        #expect(buffer.values.map(\.sequence) == [2, 3, 4])
        #expect(buffer.newestCursor == 4)
    }

    @Test("structured filters compose")
    func filters() {
        let value = OperationalEvent(
            sequence: 9,
            timestamp: Date(timeIntervalSince1970: 100),
            level: .warning,
            component: "scheduler",
            sourceName: "github",
            pipelineName: "gh",
            pipelineVersion: 3,
            runID: 42,
            node: "discover",
            module: "github.search",
            event: "run.blocked",
            message: "Waiting for rate limit")

        #expect(OperationalEventFilter(
            levels: [.warning],
            components: ["scheduler"],
            sourceName: "github",
            pipelineName: "gh",
            runID: 42,
            nodeOrModule: "github.search",
            text: "rate LIMIT"
        ).includes(value))
        #expect(!OperationalEventFilter(levels: [.error]).includes(value))
        #expect(!OperationalEventFilter(sourceName: "arxiv").includes(value))
    }

    @Test("time, Node, and Module filters remain independent")
    func detailedFilters() {
        let value = OperationalEvent(
            sequence: 10,
            timestamp: Date(timeIntervalSince1970: 100),
            level: .info,
            component: "runner",
            node: "extract",
            module: "html.extract",
            event: "task.running",
            message: "Extracting"
        )

        #expect(OperationalEventFilter(
            node: "extract",
            module: "html.extract",
            startedAt: Date(timeIntervalSince1970: 90),
            endedAt: Date(timeIntervalSince1970: 110)
        ).includes(value))
        #expect(!OperationalEventFilter(node: "store").includes(value))
        #expect(!OperationalEventFilter(
            endedAt: Date(timeIntervalSince1970: 99)
        ).includes(value))
    }

    @Test("server history query carries every canonical Console filter")
    func serverQuery() {
        let query = LogQuery(
            filter: OperationalEventFilter(
                levels: [.error],
                components: ["runner"],
                sourceName: "docs",
                pipelineName: "crawl",
                runID: 44,
                node: "extract",
                module: "html.extract",
                startedAt: Date(timeIntervalSince1970: 100),
                endedAt: Date(timeIntervalSince1970: 200),
                text: "timeout"
            ),
            after: 4,
            before: 99,
            limit: 2_000
        )
        let values = Dictionary(
            uniqueKeysWithValues: query.queryItems.compactMap { item in
                item.value.map { (item.name, $0) }
            }
        )

        #expect(query.limit == 1_000)
        #expect(values["after"] == "4")
        #expect(values["before"] == "99")
        #expect(values["level"] == "error")
        #expect(values["component"] == "runner")
        #expect(values["source"] == "docs")
        #expect(values["pipeline"] == "crawl")
        #expect(values["run_id"] == "44")
        #expect(values["node"] == "extract")
        #expect(values["module"] == "html.extract")
        #expect(values["text"] == "timeout")
        #expect(values["started_at"] != nil)
        #expect(values["ended_at"] != nil)
    }

    @Test("saved Console presets round-trip")
    func presetRoundTrip() throws {
        let preset = OperationalEventFilterPreset(
            id: UUID(uuidString: "5F4F30B8-3378-4262-8385-A647E3A955DD")!,
            name: "Failed docs",
            filter: .init(
                levels: [.error, .critical],
                sourceName: "docs",
                text: "failed"
            )
        )

        let decoded = try JSONDecoder().decode(
            OperationalEventFilterPreset.self,
            from: JSONEncoder().encode(preset)
        )

        #expect(decoded == preset)
    }

    private func event(_ sequence: Int64) -> OperationalEvent {
        OperationalEvent(
            sequence: sequence,
            timestamp: Date(timeIntervalSince1970: TimeInterval(sequence)),
            level: .info,
            component: "worker",
            event: "task.finished",
            message: "Finished")
    }
}
