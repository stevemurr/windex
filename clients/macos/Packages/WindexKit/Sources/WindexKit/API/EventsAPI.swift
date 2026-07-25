import Foundation

/// One decoded message from the dashboard stream.
///
/// The server multiplexes six feeds down one connection at different cadences —
/// `stats` every ~2s, `workers` every tick, `jobs` and `logsizes` every third,
/// `timeseries` every eighth, and `recent` only when it actually changes. A
/// consumer switches on a case rather than comparing event-name strings, so a
/// typo can't silently drop a feed.
public enum DashboardEvent: Sendable {
    /// Corpus counts, queue depths, control flags, stages, breaker, uptime.
    case stats(JSONValue)
    /// Newest documents. Only sent when the head of the feed moves.
    case recent([RecentDoc])
    /// Per-minute throughput, trailing hour.
    case timeseries([TimeseriesPoint])
    case jobs([JobInfo])
    /// Log sizes and availability, for the log catalogue.
    case logSizes([LogSource])
    case workers(WorkersState)
    /// An event name this client doesn't model. Carried rather than dropped so a
    /// server that adds a feed is visible in a debug view before it is supported.
    case unknown(name: String, data: String)

    /// The wire name, useful for logging.
    public var name: String {
        switch self {
        case .stats: return "stats"
        case .recent: return "recent"
        case .timeseries: return "timeseries"
        case .jobs: return "jobs"
        case .logSizes: return "logsizes"
        case .workers: return "workers"
        case .unknown(let name, _): return name
        }
    }
}

extension WindexClient {

    /// The live dashboard stream.
    ///
    /// Decoding happens per event, and a single malformed payload is skipped
    /// rather than tearing down the connection: losing one `stats` tick is
    /// recoverable — the next arrives in two seconds — whereas dropping the
    /// stream turns a transient server hiccup into a dead dashboard.
    ///
    /// - Parameter ticks: bound the stream to N iterations. For tests; omit in
    ///   the app, where the stream should run until the view goes away.
    public func dashboardEvents(ticks: Int? = nil) throws
        -> AsyncThrowingStream<DashboardEvent, any Error> {
        let query = ticks.map { [URLQueryItem(name: "ticks", value: String($0))] } ?? []
        let raw = try events("/v1/events", surface: .admin, query: query)

        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await event in raw {
                        if let decoded = Self.decodeDashboard(event) {
                            continuation.yield(decoded)
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// Returns nil when the payload doesn't match its event name — a skipped
    /// tick, not a fatal error. Exposed for the parser tests.
    static func decodeDashboard(_ event: SSEEvent) -> DashboardEvent? {
        switch event.event {
        case "stats":
            return (try? event.decode(JSONValue.self)).map(DashboardEvent.stats)
        case "recent":
            return (try? event.decode([RecentDoc].self)).map(DashboardEvent.recent)
        case "timeseries":
            return (try? event.decode([TimeseriesPoint].self))
                .map(DashboardEvent.timeseries)
        case "jobs":
            return (try? event.decode([JobInfo].self)).map(DashboardEvent.jobs)
        case "logsizes":
            return (try? event.decode([LogSource].self)).map(DashboardEvent.logSizes)
        case "workers":
            return (try? event.decode(WorkersState.self)).map(DashboardEvent.workers)
        default:
            return .unknown(name: event.event, data: event.data)
        }
    }
}
