import Foundation

public enum OperationalEventLevel: String, Codable, CaseIterable, Hashable, Sendable {
    case trace
    case debug
    case info
    case warning
    case error
    case critical
}

/// One structured row in the durable operational journal.
public struct OperationalEvent: Codable, Hashable, Identifiable, Sendable {
    public var id: Int64 { sequence }

    public let sequence: Int64
    public let timestamp: Date
    public let level: OperationalEventLevel
    public let component: String
    public let sourceName: String?
    public let pipelineName: String?
    public let pipelineVersion: Int?
    public let runID: Int?
    public let taskID: Int?
    public let node: String?
    public let module: String?
    public let event: String
    public let message: String
    public let data: [String: JSONValue]

    public init(
        sequence: Int64,
        timestamp: Date,
        level: OperationalEventLevel,
        component: String,
        sourceName: String? = nil,
        pipelineName: String? = nil,
        pipelineVersion: Int? = nil,
        runID: Int? = nil,
        taskID: Int? = nil,
        node: String? = nil,
        module: String? = nil,
        event: String,
        message: String,
        data: [String: JSONValue] = [:]
    ) {
        self.sequence = sequence
        self.timestamp = timestamp
        self.level = level
        self.component = component
        self.sourceName = sourceName
        self.pipelineName = pipelineName
        self.pipelineVersion = pipelineVersion
        self.runID = runID
        self.taskID = taskID
        self.node = node
        self.module = module
        self.event = event
        self.message = message
        self.data = data
    }
}

public struct OperationalEventFilter: Codable, Hashable, Sendable {
    public var levels: Set<OperationalEventLevel>
    public var components: Set<String>
    public var sourceName: String?
    public var pipelineName: String?
    public var runID: Int?
    public var nodeOrModule: String?
    public var text: String

    public init(
        levels: Set<OperationalEventLevel> = [],
        components: Set<String> = [],
        sourceName: String? = nil,
        pipelineName: String? = nil,
        runID: Int? = nil,
        nodeOrModule: String? = nil,
        text: String = ""
    ) {
        self.levels = levels
        self.components = components
        self.sourceName = sourceName
        self.pipelineName = pipelineName
        self.runID = runID
        self.nodeOrModule = nodeOrModule
        self.text = text
    }

    public func includes(_ value: OperationalEvent) -> Bool {
        if !levels.isEmpty, !levels.contains(value.level) { return false }
        if !components.isEmpty, !components.contains(value.component) { return false }
        if let sourceName, value.sourceName != sourceName { return false }
        if let pipelineName, value.pipelineName != pipelineName { return false }
        if let runID, value.runID != runID { return false }
        if let nodeOrModule,
           value.node != nodeOrModule && value.module != nodeOrModule {
            return false
        }
        let query = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return true }
        return value.message.localizedCaseInsensitiveContains(query)
            || value.event.localizedCaseInsensitiveContains(query)
            || value.component.localizedCaseInsensitiveContains(query)
            || value.sourceName?.localizedCaseInsensitiveContains(query) == true
            || value.pipelineName?.localizedCaseInsensitiveContains(query) == true
            || value.node?.localizedCaseInsensitiveContains(query) == true
            || value.module?.localizedCaseInsensitiveContains(query) == true
    }
}

/// A cursor-deduplicating, bounded buffer suitable for a Console viewport.
public struct OperationalEventBuffer: Sendable {
    public let capacity: Int
    public private(set) var values: [OperationalEvent] = []

    public init(capacity: Int = 5_000) {
        self.capacity = max(1, capacity)
    }

    public var newestCursor: Int64? {
        values.last?.sequence
    }

    public mutating func append(_ incoming: [OperationalEvent]) {
        guard !incoming.isEmpty else { return }
        var known = Set(values.map(\.sequence))
        values.append(contentsOf: incoming.filter { known.insert($0.sequence).inserted })
        values.sort { $0.sequence < $1.sequence }
        if values.count > capacity {
            values.removeFirst(values.count - capacity)
        }
    }

    public mutating func clear() {
        values.removeAll(keepingCapacity: true)
    }
}
