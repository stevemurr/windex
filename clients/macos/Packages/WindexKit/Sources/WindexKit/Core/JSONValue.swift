import Foundation

/// A decoded JSON value of unknown shape.
///
/// Needed because the settings contract is deliberately untyped at the edges: a
/// `Param`'s `default`, `prefill` and current `value` are whatever that param's
/// `kind` implies — an `Int` for `int`, a `[String]` for `url_list`, a `String`
/// for `csv` (which is stored as the raw comma-separated form, not a list). The
/// server describes the type in `kind`/`type` rather than in the JSON shape, so a
/// client that wants one `SettingsField` struct has to hold the value as-is and
/// interpret it against the kind. Modelling these as `Any` would work and then
/// lose `Sendable`, `Equatable` and round-tripping.
public enum JSONValue: Sendable, Hashable, Codable {
    case null
    case bool(Bool)
    case int(Int)
    case double(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])

    /// Equality is NUMERIC across `int` and `double`, so `.int(3) == .double(3.0)`.
    ///
    /// JSON has one number type and the case split here is a decoding artefact:
    /// a `float` param whose value happens to be `3.0` arrives as `.int(3)`
    /// (Int is tried first, so an integral value round-trips as an integer).
    /// Case-wise equality would then make a form think 3.0 became 3 — and the
    /// visible damage is a false "Adjusted to 3 — the operator's floor" on a
    /// value the server never touched.
    public static func == (lhs: JSONValue, rhs: JSONValue) -> Bool {
        switch (lhs, rhs) {
        case (.null, .null):
            return true
        case let (.bool(a), .bool(b)):
            return a == b
        case let (.string(a), .string(b)):
            return a == b
        case let (.array(a), .array(b)):
            return a == b
        case let (.object(a), .object(b)):
            return a == b
        case let (.int(a), .int(b)):
            return a == b
        case let (.double(a), .double(b)):
            return a == b
        case let (.int(a), .double(b)), let (.double(b), .int(a)):
            return Double(a) == b
        default:
            return false
        }
    }

    /// Must agree with `==`: equal values need equal hashes, so a numeric hashes
    /// on its `Double` form regardless of which case holds it.
    public func hash(into hasher: inout Hasher) {
        switch self {
        case .null:
            hasher.combine(0)
        case .bool(let v):
            hasher.combine(v)
        case .int(let v):
            hasher.combine(Double(v))
        case .double(let v):
            hasher.combine(v)
        case .string(let v):
            hasher.combine(v)
        case .array(let v):
            hasher.combine(v)
        case .object(let v):
            hasher.combine(v)
        }
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if c.decodeNil() {
            self = .null
        } else if let v = try? c.decode(Bool.self) {
            self = .bool(v)
        } else if let v = try? c.decode(Int.self) {
            // Int before Double: JSON has one number type, but `int` params round-trip
            // as integers and re-encoding 3 as 3.0 makes a PATCH body the server then
            // reads as a float.
            self = .int(v)
        } else if let v = try? c.decode(Double.self) {
            self = .double(v)
        } else if let v = try? c.decode(String.self) {
            self = .string(v)
        } else if let v = try? c.decode([JSONValue].self) {
            self = .array(v)
        } else if let v = try? c.decode([String: JSONValue].self) {
            self = .object(v)
        } else {
            throw DecodingError.dataCorruptedError(
                in: c, debugDescription: "unrepresentable JSON value")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.singleValueContainer()
        switch self {
        case .null: try c.encodeNil()
        case .bool(let v): try c.encode(v)
        case .int(let v): try c.encode(v)
        case .double(let v): try c.encode(v)
        case .string(let v): try c.encode(v)
        case .array(let v): try c.encode(v)
        case .object(let v): try c.encode(v)
        }
    }
}

// MARK: - Typed access

extension JSONValue {
    public var isNull: Bool { if case .null = self { return true }; return false }

    public var boolValue: Bool? {
        if case .bool(let v) = self { return v }
        return nil
    }

    public var intValue: Int? {
        switch self {
        case .int(let v): return v
        // A JSON number that arrived as 3.0 is still a valid `int` param value;
        // a fractional one is not, and silently truncating it would hide a
        // contract mismatch.
        case .double(let v): return v == v.rounded() ? Int(v) : nil
        default: return nil
        }
    }

    public var doubleValue: Double? {
        switch self {
        case .int(let v): return Double(v)
        case .double(let v): return v
        default: return nil
        }
    }

    public var stringValue: String? {
        if case .string(let v) = self { return v }
        return nil
    }

    public var arrayValue: [JSONValue]? {
        if case .array(let v) = self { return v }
        return nil
    }

    public var objectValue: [String: JSONValue]? {
        if case .object(let v) = self { return v }
        return nil
    }

    /// The `[String]` a `url_list` / `regex_list` param holds. Non-string members
    /// make the whole thing nil rather than silently dropping entries.
    public var stringArrayValue: [String]? {
        guard case .array(let items) = self else { return nil }
        var out: [String] = []
        out.reserveCapacity(items.count)
        for item in items {
            guard let s = item.stringValue else { return nil }
            out.append(s)
        }
        return out
    }

    /// Best-effort rendering for a text field. `csv` values are already the raw
    /// comma string, so they pass through untouched.
    public var displayString: String {
        switch self {
        case .null: return ""
        case .bool(let v): return v ? "true" : "false"
        case .int(let v): return String(v)
        case .double(let v):
            // 3.0 should read as "3", not "3.0", in a number field.
            return v == v.rounded() && abs(v) < 1e15
                ? String(Int(v)) : String(v)
        case .string(let v): return v
        case .array(let v): return v.map(\.displayString).joined(separator: ", ")
        case .object: return ""
        }
    }
}

// MARK: - Literal construction (test fixtures and form submission)

extension JSONValue: ExpressibleByNilLiteral {
    public init(nilLiteral: ()) { self = .null }
}

extension JSONValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) { self = .bool(value) }
}

extension JSONValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int) { self = .int(value) }
}

extension JSONValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: Double) { self = .double(value) }
}

extension JSONValue: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) { self = .string(value) }
}

extension JSONValue: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: JSONValue...) { self = .array(elements) }
}

extension JSONValue: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, JSONValue)...) {
        self = .object(Dictionary(uniqueKeysWithValues: elements))
    }
}
