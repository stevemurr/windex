import SwiftUI

/// Space, shape and depth from `DESIGN.md` §3.3.
public enum Layout {

    /// The 8pt scale. **Nothing between** — that is the rule, so it is a closed
    /// enum rather than a set of constants. A `CGFloat` named `space16` invites
    /// `space16 + 2` at the one call site where it looks better; this doesn't.
    public enum Space: CGFloat, Sendable, CaseIterable, Comparable {
        case xxs = 4
        case xs = 8
        case sm = 12
        case md = 16
        case lg = 24
        case xl = 32
        case xxl = 48

        public var points: CGFloat { rawValue }

        public static func < (a: Space, b: Space) -> Bool { a.rawValue < b.rawValue }
    }

    /// Corner radii. Rounded-everything is what makes an app read as generic; the
    /// flat table edges are what make it read as printed.
    public enum Radius {
        /// Controls and panels.
        public static let control: CGFloat = 4
        /// Tables, rules and the run graph. Deliberately square.
        public static let flat: CGFloat = 0
    }

    /// Hairline width. 1, never 2.
    public static let hairline: CGFloat = 1

    /// Prose caps at ~68 characters. Description fields in the recipe inspector
    /// are the only long-form text in the app and must not run the full pane
    /// width.
    public static let proseMeasure: CGFloat = 560

    /// Window minimum (§8). The three-column split collapses to two below 1100.
    public static let minimumWindow = CGSize(width: 960, height: 600)
    public static let threeColumnThreshold: CGFloat = 1100
}

extension View {
    public func padding(_ space: Layout.Space) -> some View {
        padding(space.points)
    }

    public func padding(_ edges: Edge.Set, _ space: Layout.Space) -> some View {
        padding(edges, space.points)
    }
}

extension VStack {
    public init(alignment: HorizontalAlignment = .center,
                spacing: Layout.Space,
                @ViewBuilder content: () -> Content) {
        self.init(alignment: alignment, spacing: spacing.points, content: content)
    }
}

extension HStack {
    public init(alignment: VerticalAlignment = .center,
                spacing: Layout.Space,
                @ViewBuilder content: () -> Content) {
        self.init(alignment: alignment, spacing: spacing.points, content: content)
    }
}

/// A 1px divider in `rule`.
///
/// Not SwiftUI's `Divider`, which picks its own colour and inset. Depth in this
/// design is a value step plus a hairline — there are **no shadows**, because
/// shadows on a dark ground produce mud.
public struct Hairline: View {
    @Environment(\.windexTheme) private var theme
    private let axis: Axis

    public init(_ axis: Axis = .horizontal) {
        self.axis = axis
    }

    public var body: some View {
        Rectangle()
            .fill(theme.palette.rule)
            .frame(width: axis == .vertical ? Layout.hairline : nil,
                   height: axis == .horizontal ? Layout.hairline : nil)
    }
}
