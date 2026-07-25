import Foundation

/// Where a setting's current value came from.
///
/// This is what makes "revert" meaningful in the UI: only a `.db` value has an
/// override to drop, and reverting it falls back to `.env` and then `.default`.
public enum SettingOrigin: String, Sendable, Hashable, Codable {
    /// The code default in `Settings`.
    case `default`
    /// Set in the environment (.env / compose). Not editable away, only shadowed.
    case env
    /// A runtime override in `source_config`. This is the only one DELETE removes.
    case db

    /// Whether `DELETE /settings/{scope}/{key}` would change anything.
    public var isOverride: Bool { self == .db }
}

/// One row of a settings form: the `Param` schema plus its current effective
/// value and provenance.
///
/// The server flattens these into a single JSON object (`{**field.describe(),
/// "value": ..., "origin": ...}` in `service.source_settings`), so `param`
/// decodes from the *same* container rather than a nested key.
///
/// Hand-written rather than generated, and `Tools/normalize_spec.py` removes the
/// generated twin so there is exactly one decoder. The spec types this shape only
/// here; `JobInfo.params` and `Registry.modules[]` deliver the same
/// `Param.describe()` payload as untyped JSON, so a generated `SettingsField`
/// could never be the single source of truth — it would just add a second decoder
/// for a format `Param` already has to read.
public struct SettingsField: Sendable, Hashable, Codable, Identifiable {
    public let param: Param
    /// The effective value right now. Absent for a `secret` param, which is
    /// write-only and never echoed on read.
    public let value: JSONValue?
    public let origin: SettingOrigin?

    public var id: String { param.key }
    public var key: String { param.key }

    private enum CodingKeys: String, CodingKey {
        case value, origin
    }

    public init(from decoder: Decoder) throws {
        param = try Param(from: decoder)
        let c = try decoder.container(keyedBy: CodingKeys.self)
        value = try c.decodeIfPresent(JSONValue.self, forKey: .value)
        // An unrecognised origin is not worth failing a whole form over; it only
        // costs the revert affordance for that one row.
        origin = try c.decodeIfPresent(SettingOrigin.self, forKey: .origin)
    }

    public func encode(to encoder: Encoder) throws {
        try param.encode(to: encoder)
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encodeIfPresent(value, forKey: .value)
        try c.encodeIfPresent(origin, forKey: .origin)
    }

    /// What the form control should start with: the live value, falling back to
    /// the param's prefill/default when there isn't one (a secret, typically).
    public var formValue: JSONValue? { value ?? param.initialValue }
}

/// Every editable setting under one scope. A scope is a source name (`ccnews`,
/// `wiki`, `gh`, …) or `_global` for the non-per-source knobs.
public struct SettingsScope: Sendable, Hashable, Codable, Identifiable {
    public let scope: String
    public let fields: [SettingsField]

    public var id: String { scope }

    /// The scope key for settings that aren't per-source (`settings_schema.GLOBAL`).
    public static let global = "_global"

    public var isGlobal: Bool { scope == Self.global }

    /// Fields grouped by `section`, preserving the server's field order both
    /// between and within groups — the schema's declaration order is meaningful
    /// (related knobs are adjacent) and re-sorting alphabetically loses that.
    public var sections: [(name: String?, fields: [SettingsField])] {
        var order: [String?] = []
        var groups: [String?: [SettingsField]] = [:]
        for field in fields {
            let name = field.param.section
            if groups[name] == nil { order.append(name) }
            groups[name, default: []].append(field)
        }
        return order.map { ($0, groups[$0] ?? []) }
    }
}

/// `GET /v1/settings` — every scope at once.
public struct AllSettings: Sendable, Hashable, Codable {
    public let scopes: [SettingsScope]

    public subscript(scope: String) -> SettingsScope? {
        scopes.first { $0.scope == scope }
    }
}

/// `PATCH /v1/settings/{scope}` body.
///
/// All-or-nothing on the server: one bad key rejects the batch, so a form submit
/// never lands half-applied.
public struct SettingsPatch: Sendable, Hashable, Codable {
    public let values: [String: JSONValue]

    public init(values: [String: JSONValue]) {
        self.values = values
    }
}
