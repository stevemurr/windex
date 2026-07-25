import SwiftUI

/// The colour tokens from `DESIGN.md` §3.1.
///
/// Named for the metaphor rather than the value, because that is what keeps
/// usage honest — you reach for `rule` when you want a hairline, not for
/// "gray-700". A token named after its appearance gets used wherever that
/// appearance is convenient, and the system stops meaning anything.
///
/// **The rule that matters most:** colour means *something needs you*. Health is
/// the default state, and a design that colours the default state spends its
/// loudest signal on its least informative moment. Healthy is `paper` and
/// `graphite`. `moss` exists and should be mostly absent.
public struct Palette: Sendable, Equatable {

    // MARK: Ground and ink

    /// The ground. A blue-black, never `#000` — pure black on a non-OLED display
    /// reads as a hole, and the blue cast is what makes the paper tone look warm.
    public let ink: Color
    /// Raised surfaces: panels, sidebar, popovers.
    public let plate: Color
    /// Hairlines, table dividers, input borders. 1px, never 2.
    public let rule: Color
    /// Secondary text, labels, disabled. Never below 12pt — see ``Typography``.
    public let graphite: Color
    /// Primary text. Warm off-white, not `#FFF` — the whole point of the palette.
    public let paper: Color
    /// The single accent. Process cyan, from a printer's registration mark.
    public let cyan: Color

    // MARK: Semantic — state only, never decoration

    /// Attention: paused, clamped, degraded, stale.
    public let amber: Color
    /// Fault: failed, unreachable, refused.
    public let rust: Color
    /// Healthy. Used sparingly, mostly absent.
    public let moss: Color

    // MARK: Instances

    /// The designed target. Long dwell, dense data, instant recognition — the
    /// same call Logic and DaVinci make.
    public static let dark = Palette(
        ink:      Color(hex: 0x10131A),
        plate:    Color(hex: 0x171B24),
        rule:     Color(hex: 0x262C38),
        graphite: Color(hex: 0x79808F),
        paper:    Color(hex: 0xE9E5DB),
        cyan:     Color(hex: 0x35B4D8),
        amber:    Color(hex: 0xD99A2B),
        rust:     Color(hex: 0xC4553D),
        moss:     Color(hex: 0x6E9B7A)
    )

    /// A courtesy, not a co-equal target (§3.5). Cyan is darkened for contrast on
    /// a light ground; the semantics darken by ~12%.
    public static let light = Palette(
        ink:      Color(hex: 0xF2EFE7),
        plate:    Color(hex: 0xFFFFFF),
        rule:     Color(hex: 0xDAD5C9),
        graphite: Color(hex: 0x6B7280),
        paper:    Color(hex: 0x171B24),
        cyan:     Color(hex: 0x1B87A8),
        amber:    Color(hex: 0xBF8826),
        rust:     Color(hex: 0xAC4B36),
        moss:     Color(hex: 0x61886B)
    )
}

extension Color {
    /// `0xRRGGBB`, because the tokens are specified as hex and transcribing them
    /// into decimal triples is a needless place to introduce a typo.
    init(hex: UInt32) {
        self.init(
            .sRGB,
            red: Double((hex >> 16) & 0xFF) / 255,
            green: Double((hex >> 8) & 0xFF) / 255,
            blue: Double(hex & 0xFF) / 255,
            opacity: 1
        )
    }
}
