import SwiftUI

/// The type scale from `DESIGN.md` §3.2 — three roles, three faces.
///
/// Display is **Archivo Condensed** (newsprint/highway lineage), UI is **SF Pro**
/// (a native app that fights the platform's UI face feels like a web page in a
/// window), data is **IBM Plex Mono** (typewriter lineage — it makes machine
/// output look *typed*, which is the metaphor).
///
/// Archivo's variable face and IBM Plex Mono Regular are bundled by the app under
/// `Resources/Fonts`. Every style still has a documented system fallback so a
/// missing resource degrades a screen rather than crashing it; use
/// ``Typography/missingFonts()`` in a debug build to catch packaging mistakes.
public enum Typography {

    /// A face the design calls for, and what to fall back to when it isn't
    /// installed.
    public enum FontFamily: Sendable {
        /// Archivo Condensed — set numbers, screen titles, section mastheads.
        case display
        /// Archivo Expanded — the wordmark only.
        case wordmark
        /// SF Pro — every label, button, menu, body sentence.
        case ui
        /// IBM Plex Mono — doc ids, URLs, paths, counts, timestamps, log lines.
        case data

        /// The PostScript name to look for in the bundle.
        var postScriptName: String? {
            switch self {
            // One variable face supplies both roles. Width is selected below;
            // this named Regular instance is the stable availability probe.
            case .display, .wordmark: return "ArchivoRoman-Regular"
            case .ui: return nil            // the system face; never a lookup
            case .data: return "IBMPlexMono-Regular"
            }
        }

        func resolve(size: CGFloat, weight: Font.Weight) -> Font {
            if let name = postScriptName, Typography.isAvailable(name) {
                let font = Font.custom(name, size: size).weight(weight)
                switch self {
                case .display: return font.width(.condensed)
                case .wordmark: return font.width(.expanded)
                case .data, .ui: return font
                }
            }
            switch self {
            case .display, .wordmark:
                // SF Pro's condensed width is the closest native stand-in for a
                // condensed grotesque. Not Archivo, but recognisably the same
                // register — narrow, functional, editorial.
                return .system(size: size, weight: weight).width(.condensed)
            case .ui:
                return .system(size: size, weight: weight)
            case .data:
                return .system(size: size, weight: weight, design: .monospaced)
            }
        }
    }

    /// Which of the design's faces are missing from the bundle. Empty when all
    /// are present. For a debug affordance, not a runtime branch.
    public static func missingFonts() -> [String] {
        var seen = Set<String>()
        return [FontFamily.display, .wordmark, .data]
            .compactMap(\.postScriptName)
            .filter { seen.insert($0).inserted }
            .filter { !isAvailable($0) }
    }

    static func isAvailable(_ postScriptName: String) -> Bool {
        #if canImport(AppKit)
        NSFont(name: postScriptName, size: 12) != nil
        #else
        false
        #endif
    }

    // MARK: - The scale
    //
    // Numerals are TABULAR in every style, no exceptions. A count that reflows as
    // it ticks is the single most common way a live dashboard feels cheap, and on
    // this app almost every number is live.

    /// 56/52, -2% tracking. The one number on a screen that matters.
    public static let setXL = style(.display, 56, weight: .medium, tracking: -1.12)
    /// 34/34. Secondary set numbers.
    public static let setLG = style(.display, 34, weight: .medium)
    /// 19/24, +6% tracking, uppercase. Screen titles and section heads.
    public static let masthead = style(.display, 19, weight: .semibold, tracking: 1.14)
    /// 10/12, +12% tracking, uppercase, `graphite`. Field groups, table headers.
    public static let eyebrow = style(.ui, 10, weight: .semibold, tracking: 1.2)
    /// 13/18. Everything conversational.
    public static let body = style(.ui, 13, weight: .regular)
    /// 12/16. Controls.
    public static let label = style(.ui, 12, weight: .medium)
    /// 12/17. Ids, paths, counts.
    public static let data = style(.data, 12, weight: .regular)
    /// 11/15. Log and unit-feed lines.
    public static let dataSM = style(.data, 11, weight: .regular)

    /// One resolved text style: a font plus the tracking the scale specifies.
    public struct Style: Sendable {
        public let font: Font
        public let tracking: CGFloat
        /// Whether the design sets this style in caps. The transform is applied
        /// by ``Text/windexStyle(_:)`` rather than expected of every call site,
        /// because a masthead that someone forgot to uppercase is the kind of
        /// inconsistency nobody notices until the screen looks wrong.
        public let uppercase: Bool
    }

    private static func style(_ family: FontFamily, _ size: CGFloat,
                              weight: Font.Weight, tracking: CGFloat = 0) -> Style {
        Style(font: family.resolve(size: size, weight: weight).monospacedDigit(),
              tracking: tracking,
              // Per §3.2: masthead and eyebrow are the uppercase styles.
              uppercase: size == 19 || size == 10)
    }
}

extension Text {
    /// Apply a scale entry, including its tracking and case transform.
    public func windexStyle(_ style: Typography.Style) -> Text {
        // `Text` can't transform its own content, so uppercasing happens at the
        // View level below; here we only carry font + tracking.
        self.font(style.font).tracking(style.tracking)
    }
}

extension View {
    /// Apply a scale entry to anything that renders text.
    public func windexStyle(_ style: Typography.Style) -> some View {
        self.font(style.font).tracking(style.tracking)
    }
}

/// A `Text` that honours a style's case transform as well as its metrics.
///
/// Exists because `Text.textCase(.uppercase)` is a `View` modifier and cannot be
/// folded into a `Text`-returning helper, so a masthead built the obvious way
/// silently loses its uppercase.
public struct StyledText: View {
    private let content: String
    private let style: Typography.Style

    public init(_ content: String, _ style: Typography.Style) {
        self.content = content
        self.style = style
    }

    public var body: some View {
        Text(style.uppercase ? content.uppercased() : content)
            .font(style.font)
            .tracking(style.tracking)
    }
}
