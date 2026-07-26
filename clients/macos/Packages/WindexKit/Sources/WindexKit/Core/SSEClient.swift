import Foundation

/// One decoded Server-Sent Event.
public struct SSEEvent: Sendable, Hashable {
    /// The `event:` name. SSE's default when the server omits it is `"message"`.
    public let event: String
    /// The `data:` payload, with multiple `data:` lines joined by newline per spec.
    public let data: String
    public let id: String?
    /// The server's requested reconnection delay, if it sent `retry:`.
    public let retry: Int?

    public init(event: String = "message", data: String,
                id: String? = nil, retry: Int? = nil) {
        self.event = event
        self.data = data
        self.id = id
        self.retry = retry
    }

    /// Decode the `data` payload as JSON. windex's control and log streams send
    /// a JSON object per event.
    public func decode<T: Decodable>(_ type: T.Type = T.self) throws -> T {
        do {
            return try JSONDecoder().decode(T.self, from: Data(data.utf8))
        } catch {
            throw WindexError.decoding(underlying: error)
        }
    }
}

/// Server-Sent Events over `URLSession.bytes`.
///
/// Hand-rolled rather than line-splitting on `\n\n`, because the framing rules
/// that matter are easy to get wrong and produce corruption only under load:
/// an event ends at a BLANK line, `data:` may appear repeatedly within one event
/// and joins with newlines, a leading space after the colon is stripped, and a
/// `:` line is a comment (windex's streams send those as keep-alives, so
/// treating one as an event would inject junk into the UI).
enum SSEClient {

    static func stream(request: URLRequest,
                       session: URLSession) -> AsyncThrowingStream<SSEEvent, any Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    let (bytes, response) = try await session.bytes(for: request)

                    if let http = response as? HTTPURLResponse,
                       !(200..<300).contains(http.statusCode) {
                        // Drain the error body so the operator gets the server's
                        // message rather than a bare status code.
                        var body = Data()
                        for try await byte in bytes { body.append(byte) }
                        throw WindexClient.error(status: http.statusCode, body: body)
                    }

                    // Split on newlines by hand rather than using `bytes.lines`.
                    // `AsyncLineSequence` DROPS empty lines — and in SSE the
                    // blank line is the event terminator, so every event after
                    // the first would silently merge into its predecessor.
                    var parser = SSEParser()
                    var line: [UInt8] = []
                    for try await byte in bytes {
                        guard byte == UInt8(ascii: "\n") else {
                            line.append(byte)
                            continue
                        }
                        if let event = parser.consume(
                            line: String(decoding: line, as: UTF8.self)) {
                            continuation.yield(event)
                        }
                        line.removeAll(keepingCapacity: true)
                    }
                    // A trailing event with no final blank line still counts.
                    if !line.isEmpty,
                       let event = parser.consume(
                           line: String(decoding: line, as: UTF8.self)) {
                        continuation.yield(event)
                    }
                    if let event = parser.flush() {
                        continuation.yield(event)
                    }
                    continuation.finish()
                } catch is CancellationError {
                    continuation.finish()
                } catch let error as WindexError {
                    continuation.finish(throwing: error)
                } catch {
                    continuation.finish(throwing: WindexError.transport(underlying: error))
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}

/// Incremental SSE frame parser. Split out from the transport so the framing
/// rules are testable without a socket.
struct SSEParser {
    private var event: String?
    private var dataLines: [String] = []
    private var id: String?
    private var retry: Int?

    /// Feed one line. Returns an event when the line completed one (i.e. it was
    /// blank and something had accumulated).
    mutating func consume(line: String) -> SSEEvent? {
        // Tolerate CRLF: `bytes.lines` splits on \n and leaves the \r.
        let line = line.hasSuffix("\r") ? String(line.dropLast()) : line

        if line.isEmpty {
            return flush()
        }
        // A line starting with ':' is a comment — windex uses these as
        // keep-alives to hold the connection open through idle periods.
        if line.hasPrefix(":") { return nil }

        let field: String
        var value: String
        if let colon = line.firstIndex(of: ":") {
            field = String(line[line.startIndex..<colon])
            value = String(line[line.index(after: colon)...])
            // Exactly one leading space is part of the framing, not the data.
            if value.hasPrefix(" ") { value.removeFirst() }
        } else {
            field = line
            value = ""
        }

        switch field {
        case "event": event = value
        case "data": dataLines.append(value)
        case "id": id = value
        case "retry": retry = Int(value)
        default: break        // unknown fields are ignored per spec
        }
        return nil
    }

    /// Emit whatever has accumulated, if anything.
    mutating func flush() -> SSEEvent? {
        guard !dataLines.isEmpty || event != nil else { return nil }
        let result = SSEEvent(
            event: event ?? "message",
            data: dataLines.joined(separator: "\n"),
            id: id,
            retry: retry
        )
        event = nil
        dataLines = []
        // `id` and `retry` deliberately persist across events: SSE defines them
        // as connection-level state (the last id is what a reconnect resumes
        // from), unlike `event`/`data` which are per-event.
        return result
    }
}
