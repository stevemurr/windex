import Foundation
import Observation
import WindexKit

/// The state behind a ``SchemaForm``: values, visibility, validation, and what
/// to submit.
///
/// Deliberately free of SwiftUI so the rules that matter — `dependsOn` gating,
/// clamp-vs-reject, which keys are dirty — are testable without rendering
/// anything. The view is a projection of this; all the judgement lives here.
@MainActor
@Observable
public final class FormModel {

    /// The controls, in the server's declared order. That order is meaningful —
    /// related knobs are adjacent — so it is never re-sorted.
    public let params: [Param]

    /// Current editor values, keyed by `Param.key`.
    public private(set) var values: [String: JSONValue]

    /// What the form opened with. Submitting only the difference is what makes a
    /// PATCH minimal, and it is how ``isDirty`` is answered.
    private var baseline: [String: JSONValue]

    /// Server-reported adjustments from the last save, keyed by param.
    ///
    /// A clamped value comes back different from what was sent, so the form has
    /// to say why — "Adjusted to 3.0 — the operator's floor" — or the operator
    /// types 0.5, sees 3.0, and has no idea what happened.
    public private(set) var clampNotices: [String: String] = [:]

    /// Per-field errors from the server's last 422, keyed by param.
    public private(set) var serverErrors: [String: String] = [:]

    public init(params: [Param], values: [String: JSONValue] = [:]) {
        self.params = params
        // A param with no supplied value starts at its prefill, then its default.
        var seeded = values
        for param in params where seeded[param.key] == nil {
            if let initial = param.initialValue {
                seeded[param.key] = initial
            }
        }
        self.values = seeded
        self.baseline = seeded
    }

    /// Build from a settings scope, which carries both schema and live values.
    public convenience init(scope: SettingsScope) {
        var values: [String: JSONValue] = [:]
        for field in scope.fields {
            if let value = field.value { values[field.key] = value }
        }
        self.init(params: scope.fields.map(\.param), values: values)
    }

    /// Build from a job's parameter schema. Job params are `enforce: .reject`, so
    /// ``error(for:)`` will block an out-of-range value here where a settings
    /// form would accept it.
    public convenience init(jobParameters: [String: Param]) {
        // Dictionary order is not stable; sort by key so the dialog doesn't
        // reshuffle its fields between openings.
        self.init(params: jobParameters.values.sorted { $0.key < $1.key })
    }

    // MARK: - Reading

    public func value(for param: Param) -> JSONValue? {
        values[param.key]
    }

    public func value(forKey key: String) -> JSONValue? {
        values[key]
    }

    /// Whether this control's `dependsOn` condition is met.
    ///
    /// A control whose condition fails is **dimmed and disabled, never hidden**
    /// (§5.1). Hiding it means an operator looking for a setting they know exists
    /// simply cannot find it; showing it disabled teaches what the system will
    /// not do and why.
    public func isSatisfied(_ param: Param) -> Bool {
        guard let rule = param.dependsOn else { return true }
        return rule.isSatisfied(by: values[rule.field])
    }

    /// Whether the control accepts input: not locked, and its dependency met.
    public func isEnabled(_ param: Param) -> Bool {
        param.isEditable && isSatisfied(param)
    }

    /// Helper text under the control: the locked reason, the dependency, the
    /// clamp note, or the description — in that order of usefulness.
    public func helperText(for param: Param) -> String? {
        if let reason = param.lockedReason { return reason }
        if !isSatisfied(param), let rule = param.dependsOn {
            return "Requires \(rule.field)."
        }
        if let notice = clampNotices[param.key] { return notice }
        if let note = param.clampNote { return note }
        return param.description.isEmpty ? nil : param.description
    }

    /// The error to show on the control, if any. Server errors win over local
    /// ones — the server is the authority, and its message is the specific one.
    public func error(for param: Param) -> String? {
        if let server = serverErrors[param.key] { return server }
        guard let value = values[param.key] else {
            return param.required ? "\(param.title) is required" : nil
        }
        return param.validate(value)
    }

    /// What the value will become on save, when the server would clamp it.
    ///
    /// Only ever non-nil for `enforce: .clamp` params. Showing this while typing
    /// is what turns a silent adjustment into an expected one.
    public func clampPreview(for param: Param) -> JSONValue? {
        guard let value = values[param.key] else { return nil }
        return param.clamped(value)
    }

    /// Whether anything has changed since the form opened.
    public var isDirty: Bool { !changes.isEmpty }

    /// Just the changed keys — the minimal PATCH body.
    ///
    /// A settings PATCH merges, so sending unchanged keys would work but would
    /// also convert every untouched default into an explicit override, and the
    /// origin column would go all-`db` after one save.
    public var changes: [String: JSONValue] {
        var out: [String: JSONValue] = [:]
        for param in params {
            // A disabled control's value is not the operator's intent.
            guard isEnabled(param) else { continue }
            let current = values[param.key]
            if current != baseline[param.key], let current {
                out[param.key] = current
            }
        }
        return out
    }

    /// Local errors across the whole form, in field order.
    public var errors: [(param: Param, message: String)] {
        params.compactMap { param in
            guard isEnabled(param), let message = error(for: param) else { return nil }
            return (param, message)
        }
    }

    /// Whether a submit should be allowed. `enforce: .clamp` params never block
    /// it — an out-of-range number there is valid input the server will adjust.
    public var canSubmit: Bool { isDirty && errors.isEmpty }

    // MARK: - Writing

    public func set(_ key: String, _ value: JSONValue?) {
        if let value {
            values[key] = value
        } else {
            values.removeValue(forKey: key)
        }
        // A field the operator has just touched should not still be showing the
        // last submit's complaint about it.
        serverErrors.removeValue(forKey: key)
        clampNotices.removeValue(forKey: key)
    }

    public func set(_ param: Param, _ value: JSONValue?) {
        set(param.key, value)
    }

    /// Discard edits.
    public func reset() {
        values = baseline
        serverErrors = [:]
        clampNotices = [:]
    }

    /// Adopt the server's response as the new truth.
    ///
    /// **The response is authoritative, not the request.** A clamped value comes
    /// back different from what was sent, so a form that keeps its own input
    /// displays a lie. Where the two differ, a notice is recorded so the change
    /// is explained rather than merely applied.
    public func apply(_ fields: [SettingsField]) {
        var updated: [String: JSONValue] = [:]
        var notices: [String: String] = [:]

        for field in fields {
            guard let returned = field.value else {
                // A secret is write-only: nothing comes back, so keep what the
                // operator typed rather than blanking the control.
                updated[field.key] = values[field.key]
                continue
            }
            if let submitted = values[field.key], submitted != returned {
                notices[field.key] = Self.clampNotice(
                    for: field.param, submitted: submitted, returned: returned)
            }
            updated[field.key] = returned
        }

        values = updated
        baseline = updated
        clampNotices = notices
        serverErrors = [:]
    }

    /// Attach a 422's per-field failures to their controls.
    public func apply(_ failures: [ValidationFailure]) {
        var errors: [String: String] = [:]
        let keys = Set(params.map(\.key))
        for failure in failures {
            // FastAPI prefixes the location kind (`body`, `query`); the field is
            // whichever component names a param we know about.
            let field = failure.loc.last(where: keys.contains) ?? failure.field
            if let field { errors[field] = failure.msg }
        }
        serverErrors = errors
    }

    /// Turn a 422 into per-field errors when it carries them, and return the
    /// message to show as a banner when it doesn't.
    @discardableResult
    public func apply(_ error: any Error) -> String? {
        guard let error = error as? WindexError else {
            return error.localizedDescription
        }
        if case .validation(let failures, _) = error, !failures.isEmpty {
            apply(failures)
            // Anything not attributable to a field still needs saying.
            return failures.contains { $0.field == nil }
                ? error.localizedDescription : nil
        }
        return error.localizedDescription
    }

    static func clampNotice(for param: Param,
                            submitted: JSONValue,
                            returned: JSONValue) -> String {
        let value = returned.displayString
        let unit = param.unit.map { " \($0)" } ?? ""
        guard param.kind.isNumeric, let submitted = submitted.doubleValue,
              let returned = returned.doubleValue else {
            return "Adjusted to \(value)\(unit)."
        }
        if returned > submitted {
            return "Adjusted to \(value)\(unit) — the operator's floor."
        }
        if returned < submitted {
            return "Adjusted to \(value)\(unit) — the operator's ceiling."
        }
        return "Adjusted to \(value)\(unit)."
    }

    // MARK: - Grouping

    /// Fields grouped by `section`, in declaration order, split into the ones
    /// shown normally and the ones behind the "Advanced" disclosure.
    public var sections: [FormSection] {
        var order: [String?] = []
        var groups: [String?: [Param]] = [:]
        for param in params where param.editor.isRendered {
            let name = param.section
            if groups[name] == nil { order.append(name) }
            groups[name, default: []].append(param)
        }
        return order.map { name in
            let all = groups[name] ?? []
            return FormSection(name: name,
                               fields: all.filter { !$0.advanced },
                               advanced: all.filter(\.advanced))
        }
    }
}

/// One heading's worth of controls.
public struct FormSection: Identifiable, Sendable {
    public let name: String?
    public let fields: [Param]
    /// Rendered inside a collapsed disclosure.
    public let advanced: [Param]

    public var id: String { name ?? "" }
    public var isEmpty: Bool { fields.isEmpty && advanced.isEmpty }
}
