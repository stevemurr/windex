import Foundation

/// A `Date` decoded from any timestamp shape windex actually emits.
///
/// FastAPI serializes `datetime` with `.isoformat()`, which yields fractional
/// seconds only when the value has microseconds and a `+00:00` offset only when
/// the value is tz-aware. windex's corpus carries both: `published_at` is parsed
/// from wildly inconsistent upstream feeds (`dateparse.py` exists for this), and
/// several columns are naive. A single `ISO8601DateFormatter` handles exactly one
/// of those shapes and returns nil for the rest, which would silently drop the
/// date on a result that has one.
///
/// Naive timestamps are read as UTC — that is what the server stores.
struct FlexibleDate: Decodable, Sendable {
    let date: Date

    // `ISO8601DateFormatter` is documented thread-safe once configured, and these
    // are configured here and never mutated again — but it predates `Sendable`
    // and can't be marked as such, so the unsafe opt-out is the accurate
    // annotation rather than a workaround. Building them per-decode instead would
    // put five formatter allocations on every result of every search.
    nonisolated(unsafe) private static let formatters: [ISO8601DateFormatter] = {
        let withFraction = ISO8601DateFormatter()
        withFraction.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        let plain = ISO8601DateFormatter()
        plain.formatOptions = [.withInternetDateTime]

        // Naive (no offset): parse as UTC.
        let naiveFraction = ISO8601DateFormatter()
        naiveFraction.formatOptions = [
            .withFullDate, .withTime, .withColonSeparatorInTime,
            .withDashSeparatorInDate, .withFractionalSeconds,
        ]
        naiveFraction.timeZone = TimeZone(secondsFromGMT: 0)

        let naive = ISO8601DateFormatter()
        naive.formatOptions = [
            .withFullDate, .withTime, .withColonSeparatorInTime,
            .withDashSeparatorInDate,
        ]
        naive.timeZone = TimeZone(secondsFromGMT: 0)

        // Date only (`2026-07-24`).
        let dateOnly = ISO8601DateFormatter()
        dateOnly.formatOptions = [.withFullDate, .withDashSeparatorInDate]
        dateOnly.timeZone = TimeZone(secondsFromGMT: 0)

        return [withFraction, plain, naiveFraction, naive, dateOnly]
    }()

    init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()

        // A bare number is a unix epoch — GH Archive and the HN API both hand
        // windex integer timestamps.
        if let epoch = try? c.decode(Double.self) {
            date = Date(timeIntervalSince1970: epoch)
            return
        }

        let raw = try c.decode(String.self)
        for formatter in Self.formatters {
            if let parsed = formatter.date(from: raw) {
                date = parsed
                return
            }
        }
        throw DecodingError.dataCorruptedError(
            in: c, debugDescription: "unrecognised timestamp: \(raw)")
    }
}
