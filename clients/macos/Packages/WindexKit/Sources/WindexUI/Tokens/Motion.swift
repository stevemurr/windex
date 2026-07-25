import SwiftUI

/// Motion from `DESIGN.md` §3.4.
///
/// **Motion is reserved for things that are actually moving.** Four places, no
/// others: the throughput counter, rows entering the unit feed, a run-graph node
/// changing status, and the system's own navigation transitions.
///
/// No scroll reveals, no hover lifts, no shimmer skeletons, no spring bounces.
/// Each of those animates a moment that carries no information, and on a screen
/// where real numbers are ticking they compete with the thing worth watching.
public enum Motion {

    /// The throughput figure counting to a new value.
    public static let counter = Animation.easeOut(duration: 0.4)
    /// A new row entering the unit feed: fade plus a 4pt offset.
    public static let feedInsert = Animation.easeOut(duration: 0.18)
    /// A run-graph node crossfading to a new status colour.
    public static let nodeStatus = Animation.easeInOut(duration: 0.25)

    /// The offset a feed row enters from.
    public static let feedOffset: CGFloat = 4

    /// Nil when the operator has asked for reduced motion, so `withAnimation`
    /// and `.animation(_:value:)` become no-ops and final values appear
    /// immediately — which is the required behaviour, not a degraded one.
    public static func respecting(_ reduceMotion: Bool,
                                  _ animation: Animation) -> Animation? {
        reduceMotion ? nil : animation
    }
}

extension View {
    /// Animate only when the operator hasn't asked for less of it.
    ///
    /// Reads the environment itself so no call site can forget: the accessibility
    /// rule is honoured by construction rather than by everyone remembering.
    public func windexAnimation<V: Equatable>(_ animation: Animation,
                                              value: V) -> some View {
        modifier(ReducedMotionAnimation(animation: animation, value: value))
    }
}

private struct ReducedMotionAnimation<V: Equatable>: ViewModifier {
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    let animation: Animation
    let value: V

    func body(content: Content) -> some View {
        content.animation(Motion.respecting(reduceMotion, animation), value: value)
    }
}
