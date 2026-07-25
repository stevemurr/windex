import SwiftUI

/// The active design tokens.
///
/// Carried in the environment rather than read from static constants so light
/// mode and previews are a value swap, and so no view reaches past the system
/// for a colour.
public struct WindexTheme: Sendable, Equatable {
    public let palette: Palette

    public static let dark = WindexTheme(palette: .dark)
    public static let light = WindexTheme(palette: .light)
}

extension EnvironmentValues {
    /// Defaults to dark. The app commits to a single dark identity rather than
    /// tracking system appearance (§1) — long dwell, dense data, and an identity
    /// you recognise instantly.
    @Entry public var windexTheme: WindexTheme = .dark
}

extension View {
    public func windexTheme(_ theme: WindexTheme) -> some View {
        environment(\.windexTheme, theme)
    }

    /// The app's ground colour behind this view, edge to edge.
    public func windexBackground() -> some View {
        modifier(WindexBackground())
    }
}

private struct WindexBackground: ViewModifier {
    @Environment(\.windexTheme) private var theme

    func body(content: Content) -> some View {
        content
            .background(theme.palette.ink)
            .foregroundStyle(theme.palette.paper)
    }
}
