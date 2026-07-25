import SwiftUI

/// The one status vocabulary, from `DESIGN.md` §5.2.
///
/// Every state carries a **word**, not just a glyph and a colour. That is an
/// accessibility requirement (§8: status is never conveyed by colour alone) and
/// it is also why the palette can stay this restrained — the word does the work,
/// so the colour only has to mark urgency.
///
/// `healthy` deliberately has no word and no colour. A column of green ticks
/// carries no information and costs the interface its calm.
public enum Status: Sendable, Hashable, CaseIterable {
    /// The default. Renders as a `graphite` middot, nothing more.
    case healthy
    case running
    /// Paused, stale, clamped, degraded.
    case attention
    case fault

    public var glyph: String {
        switch self {
        case .healthy: return "·"
        case .running: return "◐"
        case .attention: return "⚠"
        case .fault: return "■"
        }
    }

    /// The default word. Callers usually pass a more specific one — "paused",
    /// "stale", "degraded" are all `attention`.
    public var word: String? {
        switch self {
        case .healthy: return nil
        case .running: return "running"
        case .attention: return "attention"
        case .fault: return "failed"
        }
    }

    func color(_ palette: Palette) -> Color {
        switch self {
        case .healthy: return palette.graphite
        case .running: return palette.cyan
        case .attention: return palette.amber
        case .fault: return palette.rust
        }
    }
}

/// Glyph plus word, in the status colour. Not a pill — badges at table density
/// become confetti (§4.2).
public struct StatusBadge: View {
    @Environment(\.windexTheme) private var theme

    private let status: Status
    private let word: String?

    /// - Parameter word: overrides the default, e.g. `"paused"` for `.attention`.
    ///   Pass `nil` to use the status's own word.
    public init(_ status: Status, word: String? = nil) {
        self.status = status
        self.word = word ?? status.word
    }

    public var body: some View {
        HStack(spacing: .xxs) {
            Text(status.glyph)
            if let word {
                Text(word)
            }
        }
        .windexStyle(Typography.label)
        .foregroundStyle(status.color(theme.palette))
        // The glyph is decorative once the word is present; without a word
        // (healthy) there is nothing to announce, so the whole thing is hidden
        // from VoiceOver rather than read as a stray bullet.
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(word ?? "")
        .accessibilityHidden(word == nil)
    }
}

/// A lowercase status word in a fixed-width column, for the unit feed (§4.2).
///
/// `data-sm`, eight characters wide, so `ok` / `skip` / `fail` align down the
/// left edge like a printed proof rather than jostling.
public struct FeedStatus: View {
    @Environment(\.windexTheme) private var theme

    private let word: String
    private let status: Status

    public init(_ word: String, _ status: Status) {
        self.word = word
        self.status = status
    }

    public var body: some View {
        Text(word)
            .windexStyle(Typography.dataSM)
            .foregroundStyle(colour)
            .frame(width: 64, alignment: .leading)
    }

    private var colour: Color {
        switch status {
        // A skipped unit is not a problem and should recede, not compete with
        // the rows that carry information.
        case .healthy: return theme.palette.graphite
        default: return status.color(theme.palette)
        }
    }
}
