import Foundation

/// One typed, bounded, self-describing parameter — the shape every form renders.
///
/// This is the Swift mirror of `windex.schema.param.Param.describe()`
/// (`src/windex/schema/param.py`). It is decoded **generically**: nothing here
/// names a windex setting. That is the whole point — the same struct backs
/// editable settings, Run arguments, and Pipeline Module configuration, so
/// `SchemaForm` is written once against `Param` and the Pipeline composer drops
/// straight onto it without a second value renderer.
///
/// Two server rules a client must not paper over:
///
/// * **Clamp, don't reject.** When `enforce == .clamp`, an out-of-range number is
///   silently pulled to the bound rather than 422'd. `clamp` says which ends, and
///   `clampNote` is the explanation to show — so "I typed 0.5 and got 1.0" is
///   never a mystery. When `enforce == .reject`, the server refuses instead, and
///   a client that helpfully clamps first would submit something other than what
///   was typed. Branch on `enforce`; never assume.
/// * **`lo`/`hi` are already operator-resolved.** The server reports
///   `min(declared_hi, operator_ceiling)`, so these are the bounds that will
///   actually be enforced. Don't re-derive them.
public struct Param: Sendable, Hashable, Codable, Identifiable {

    /// The closed set of value kinds (`KINDS` in param.py). Unknown kinds decode
    /// to `.unknown` rather than failing: a newer server adding a kind should
    /// degrade one control to a plain text field, not break the whole form.
    public enum Kind: Sendable, Hashable, Codable, RawRepresentable {
        case int, float, string, bool, csv, choice
        case date, url, urlList, regexList, secretRef, duration
        case unknown(String)

        public init?(rawValue: String) {
            switch rawValue {
            case "int": self = .int
            case "float": self = .float
            case "str": self = .string
            case "bool": self = .bool
            case "csv": self = .csv
            case "choice": self = .choice
            case "date": self = .date
            case "url": self = .url
            case "url_list": self = .urlList
            case "regex_list": self = .regexList
            case "secret_ref": self = .secretRef
            case "duration": self = .duration
            default: self = .unknown(rawValue)
            }
        }

        public var rawValue: String {
            switch self {
            case .int: return "int"
            case .float: return "float"
            case .string: return "str"
            case .bool: return "bool"
            case .csv: return "csv"
            case .choice: return "choice"
            case .date: return "date"
            case .url: return "url"
            case .urlList: return "url_list"
            case .regexList: return "regex_list"
            case .secretRef: return "secret_ref"
            case .duration: return "duration"
            case .unknown(let raw): return raw
            }
        }

        /// Whether values of this kind carry numeric bounds. Only these honour
        /// `lo`/`hi`/`clamp`.
        public var isNumeric: Bool {
            self == .int || self == .float
        }
    }

    /// Which UI control renders this param.
    ///
    /// The server's `EDITOR_FOR_KIND` supplies a default per kind, but the set is
    /// **open**: the registry passes a Module's declared `editor` string straight
    /// through, so a Pipeline can ask for a control no built-in setting uses. The
    /// cases below are the full vocabulary `DESIGN.md` §5.1 specifies a
    /// control for; anything else lands in `.unknown` and falls back to a text
    /// field, so a newer server adding a control degrades one row rather than
    /// breaking the form.
    public enum Editor: Sendable, Hashable, Codable, RawRepresentable {
        case number, textfield, checkbox, stringList, select
        case datepicker, url, regexList, secret, duration, textarea
        /// Menu of toggles, labelled from `enumTitles`.
        case multiselect
        /// Two-column table.
        case keyValue
        /// Text editor with parse-on-type and an inline error.
        case json
        /// Not rendered at all.
        case hidden
        case unknown(String)

        public init?(rawValue: String) {
            switch rawValue {
            case "number": self = .number
            case "textfield": self = .textfield
            case "checkbox": self = .checkbox
            case "stringList": self = .stringList
            case "select": self = .select
            case "datepicker": self = .datepicker
            case "url": self = .url
            case "regexList": self = .regexList
            case "secret": self = .secret
            case "duration": self = .duration
            case "textarea": self = .textarea
            case "multiselect": self = .multiselect
            case "keyValue": self = .keyValue
            case "json": self = .json
            case "hidden": self = .hidden
            default: self = .unknown(rawValue)
            }
        }

        public var rawValue: String {
            switch self {
            case .number: return "number"
            case .textfield: return "textfield"
            case .checkbox: return "checkbox"
            case .stringList: return "stringList"
            case .select: return "select"
            case .datepicker: return "datepicker"
            case .url: return "url"
            case .regexList: return "regexList"
            case .secret: return "secret"
            case .duration: return "duration"
            case .textarea: return "textarea"
            case .multiselect: return "multiselect"
            case .keyValue: return "keyValue"
            case .json: return "json"
            case .hidden: return "hidden"
            case .unknown(let raw): return raw
            }
        }

        /// A `hidden` param takes part in submission but draws nothing.
        public var isRendered: Bool { self != .hidden }
    }

    /// How the server handles an out-of-range value.
    public enum Enforce: String, Sendable, Hashable, Codable {
        /// Silently pulled to the bound. The windex default; right for settings
        /// and Pipeline Module configuration.
        case clamp
        /// Refused with a 422. Used where a value is an explicit instruction (a
        /// job argument) and running something else is worse than an error.
        case reject
    }

    /// Which ends of the range are silently enforced.
    public enum ClampEnds: String, Sendable, Hashable, Codable {
        case floor, ceiling, both

        public var clampsLow: Bool { self == .floor || self == .both }
        public var clampsHigh: Bool { self == .ceiling || self == .both }
    }

    /// Install-time vs run-time parameter (`"runtime"` for settings).
    public enum Stage: String, Sendable, Hashable, Codable {
        case install, runtime
    }

    /// A visibility condition on another field's value: `{"field": ..., "equals": ...}`
    /// or `{"field": ..., "in": [...]}`.
    public struct DependsOn: Sendable, Hashable, Codable {
        public let field: String
        public let equals: JSONValue?
        public let anyOf: [JSONValue]?

        enum CodingKeys: String, CodingKey {
            case field, equals
            case anyOf = "in"
        }

        public init(field: String, equals: JSONValue? = nil, anyOf: [JSONValue]? = nil) {
            self.field = field
            self.equals = equals
            self.anyOf = anyOf
        }

        /// Whether the dependent control should be shown, given the current value
        /// of `field` in the form. A condition naming neither `equals` nor `in` is
        /// treated as satisfied — an unparseable rule must not hide a control the
        /// operator then can't find.
        public func isSatisfied(by value: JSONValue?) -> Bool {
            if let equals { return value == equals }
            if let anyOf { return value.map(anyOf.contains) ?? false }
            return true
        }
    }

    // MARK: Identity

    /// The attribute/config name this param sets. Unique within its form.
    public let key: String
    public var id: String { key }
    public let kind: Kind

    // MARK: Presentation

    public let title: String
    public let description: String
    public let editor: Editor
    /// Groups fields under a heading. Empty means ungrouped.
    public let section: String?
    /// Rendered as a suffix ("s", "bytes").
    public let unit: String?
    /// Render inside a collapsed disclosure.
    public let advanced: Bool
    /// Write-only: never echoed on read, so an existing value shows as a
    /// placeholder rather than a populated field.
    public let secret: Bool
    /// Present ⇒ render disabled, with this as the explanation.
    public let lockedReason: String?

    // MARK: Values

    public let defaultValue: JSONValue?
    /// What a form STARTS with, distinct from `defaultValue` (what the server does
    /// when the key is absent). They differ where the default is unwieldy.
    public let prefill: JSONValue?
    public let required: Bool

    // MARK: Bounds — already tightened by the operator's ceiling/floor

    public let lo: Double?
    public let hi: Double?
    public let choices: [String]
    /// Human labels parallel to `choices`.
    public let enumTitles: [String]
    public let maxItems: Int?
    public let maxLength: Int?
    public let pattern: String?

    // MARK: Behaviour

    public let enforce: Enforce
    public let clamp: ClampEnds?
    public let clampNote: String?
    public let dependsOn: DependsOn?
    public let stage: Stage
    /// `secret_ref` only: the operator keys this param may name.
    public let allow: [String]

    // MARK: - Decoding

    private enum CodingKeys: String, CodingKey {
        case key, kind, lo, hi, choices, label, help
        case type, editor, title, description, required, advanced, secret, stage, enforce
        case defaultValue = "default"
        case prefill, section, unit, enumTitles, maxItems, maxLength, pattern
        case clamp, clampNote, lockedReason, dependsOn, allow
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        key = try c.decode(String.self, forKey: .key)
        kind = Kind(rawValue: try c.decode(String.self, forKey: .kind)) ?? .unknown("")

        // `describe()` emits both the legacy console keys (label/help) and the
        // generalized ones (title/description) with identical content. Prefer the
        // generalized pair and fall back, so this keeps decoding if the legacy
        // half is dropped once the console is deleted.
        title = try c.decodeIfPresent(String.self, forKey: .title)
            ?? c.decodeIfPresent(String.self, forKey: .label)
            ?? key
        description = try c.decodeIfPresent(String.self, forKey: .description)
            ?? c.decodeIfPresent(String.self, forKey: .help)
            ?? ""

        let rawEditor = try c.decodeIfPresent(String.self, forKey: .editor)
        editor = rawEditor.flatMap(Editor.init(rawValue:)) ?? Param.defaultEditor(for: kind)

        lo = try c.decodeIfPresent(Double.self, forKey: .lo)
        hi = try c.decodeIfPresent(Double.self, forKey: .hi)
        choices = try c.decodeIfPresent([String].self, forKey: .choices) ?? []
        enumTitles = try c.decodeIfPresent([String].self, forKey: .enumTitles) ?? []

        defaultValue = try c.decodeIfPresent(JSONValue.self, forKey: .defaultValue)
        prefill = try c.decodeIfPresent(JSONValue.self, forKey: .prefill)
        required = try c.decodeIfPresent(Bool.self, forKey: .required) ?? false
        advanced = try c.decodeIfPresent(Bool.self, forKey: .advanced) ?? false
        secret = try c.decodeIfPresent(Bool.self, forKey: .secret) ?? false

        section = try c.decodeIfPresent(String.self, forKey: .section)
        unit = try c.decodeIfPresent(String.self, forKey: .unit)
        maxItems = try c.decodeIfPresent(Int.self, forKey: .maxItems)
        maxLength = try c.decodeIfPresent(Int.self, forKey: .maxLength)
        pattern = try c.decodeIfPresent(String.self, forKey: .pattern)

        // An unrecognised enforce value must fail SAFE. `clamp` would let a client
        // quietly adjust a value the server then refuses; `reject` only costs an
        // error the operator can see.
        let rawEnforce = try c.decodeIfPresent(String.self, forKey: .enforce)
        enforce = rawEnforce.flatMap(Enforce.init(rawValue:)) ?? .reject
        clamp = try c.decodeIfPresent(ClampEnds.self, forKey: .clamp)
        clampNote = try c.decodeIfPresent(String.self, forKey: .clampNote)
        lockedReason = try c.decodeIfPresent(String.self, forKey: .lockedReason)
        dependsOn = try c.decodeIfPresent(DependsOn.self, forKey: .dependsOn)
        stage = try c.decodeIfPresent(Stage.self, forKey: .stage) ?? .runtime
        allow = try c.decodeIfPresent([String].self, forKey: .allow) ?? []
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(key, forKey: .key)
        try c.encode(kind.rawValue, forKey: .kind)
        try c.encode(title, forKey: .title)
        try c.encode(description, forKey: .description)
        try c.encode(editor.rawValue, forKey: .editor)
        try c.encode(required, forKey: .required)
        try c.encode(advanced, forKey: .advanced)
        try c.encode(secret, forKey: .secret)
        try c.encode(stage, forKey: .stage)
        try c.encode(enforce, forKey: .enforce)
        try c.encodeIfPresent(lo, forKey: .lo)
        try c.encodeIfPresent(hi, forKey: .hi)
        if !choices.isEmpty { try c.encode(choices, forKey: .choices) }
        if !enumTitles.isEmpty { try c.encode(enumTitles, forKey: .enumTitles) }
        try c.encodeIfPresent(defaultValue, forKey: .defaultValue)
        try c.encodeIfPresent(prefill, forKey: .prefill)
        try c.encodeIfPresent(section, forKey: .section)
        try c.encodeIfPresent(unit, forKey: .unit)
        try c.encodeIfPresent(maxItems, forKey: .maxItems)
        try c.encodeIfPresent(maxLength, forKey: .maxLength)
        try c.encodeIfPresent(pattern, forKey: .pattern)
        try c.encodeIfPresent(clamp, forKey: .clamp)
        try c.encodeIfPresent(clampNote, forKey: .clampNote)
        try c.encodeIfPresent(lockedReason, forKey: .lockedReason)
        try c.encodeIfPresent(dependsOn, forKey: .dependsOn)
        if !allow.isEmpty { try c.encode(allow, forKey: .allow) }
    }

    static func defaultEditor(for kind: Kind) -> Editor {
        switch kind {
        case .int, .float: return .number
        case .string: return .textfield
        case .bool: return .checkbox
        case .csv, .urlList: return .stringList
        case .choice: return .select
        case .date: return .datepicker
        case .url: return .url
        case .regexList: return .regexList
        case .secretRef: return .secret
        case .duration: return .duration
        case .unknown: return .textfield
        }
    }
}

// MARK: - Form behaviour

extension Param {
    /// Whether the control should be editable at all.
    public var isEditable: Bool { lockedReason == nil }

    /// The label to show for a choice, falling back to the raw value when the
    /// server didn't supply parallel titles.
    public func title(forChoice choice: String) -> String {
        guard let idx = choices.firstIndex(of: choice),
              idx < enumTitles.count else { return choice }
        return enumTitles[idx]
    }

    /// What an empty form field should start as: `prefill` when the server offered
    /// one (the default is unwieldy), otherwise `default`.
    public var initialValue: JSONValue? { prefill ?? defaultValue }

    /// Apply the server's clamp rule locally so the form can *preview* the
    /// adjustment before submitting.
    ///
    /// Returns `nil` when nothing would change. Only ever called for `.clamp`
    /// params — under `.reject` the server refuses out-of-range input, and
    /// adjusting it here would submit a value the operator didn't type.
    public func clamped(_ value: JSONValue) -> JSONValue? {
        guard enforce == .clamp, kind.isNumeric, let n = value.doubleValue else { return nil }
        var out = n
        if let lo, clamp?.clampsLow ?? true { out = max(out, lo) }
        if let hi, clamp?.clampsHigh ?? true { out = min(out, hi) }
        guard out != n else { return nil }
        return kind == .int ? .int(Int(out)) : .double(out)
    }

    /// Local pre-submit validation, mirroring `Param.coerce`'s rules so a form can
    /// show the error inline instead of round-tripping for a 422.
    ///
    /// This is a convenience, never the authority: `coerce()` on the server is the
    /// security boundary, and it runs regardless of what this returns.
    public func validate(_ value: JSONValue) -> String? {
        if let lockedReason { return "\(key): not editable (\(lockedReason))" }

        switch kind {
        case .bool:
            guard value.boolValue != nil else { return "\(key): expected true/false" }

        case .choice:
            guard let s = value.stringValue, choices.contains(s) else {
                return "\(key): must be one of \(choices.joined(separator: ", "))"
            }

        case .secretRef:
            guard let s = value.stringValue else { return "\(key): expected a string" }
            let name = s.trimmingCharacters(in: .whitespaces)
            if !allow.isEmpty && !allow.contains(name) {
                return "\(key): must be one of \(allow.joined(separator: ", "))"
            }

        case .string, .csv, .url, .date, .duration:
            guard let s = value.stringValue else { return "\(key): expected a string" }
            if let maxLength, s.count > maxLength {
                return "\(key): longer than \(maxLength) chars"
            }

        case .urlList, .regexList:
            guard let items = value.stringArrayValue else {
                return "\(key): expected a list of strings"
            }
            if let maxItems, items.count > maxItems {
                return "\(key): at most \(maxItems) items"
            }
            for item in items {
                if item.isEmpty { return "\(key): each item must be a non-empty string" }
                if let maxLength, item.count > maxLength {
                    return "\(key): item longer than \(maxLength) chars"
                }
            }
            if kind == .regexList {
                for pat in items where (try? NSRegularExpression(pattern: pat)) == nil {
                    return "\(key): invalid regex \(pat)"
                }
            }

        case .int, .float:
            // Python's coerce rejects bool for a numeric even though bool is an int
            // there; mirror that so the two agree on what's valid.
            if value.boolValue != nil { return "\(key): expected a number" }
            guard let n = value.doubleValue else { return "\(key): expected a number" }
            if kind == .int, case .double(let d) = value, d != d.rounded() {
                return "\(key): expected a number"
            }
            // Only `reject` produces an error; a `clamp` param out of range is
            // valid input that the server will quietly pull to the bound.
            if enforce == .reject {
                if (lo.map { n < $0 } ?? false) || (hi.map { n > $0 } ?? false) {
                    return "\(key) out of range [\(lo?.description ?? "-"), \(hi?.description ?? "-")]"
                }
            }

        case .unknown(let raw):
            return "\(key): unsupported kind '\(raw)'"
        }
        return nil
    }
}
