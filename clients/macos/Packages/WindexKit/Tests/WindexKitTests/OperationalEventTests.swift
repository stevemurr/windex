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
