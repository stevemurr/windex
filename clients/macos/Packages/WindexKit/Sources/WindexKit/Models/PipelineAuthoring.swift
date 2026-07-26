import Foundation

/// Mutable authoring shape for a Pipeline parameter declaration.
///
/// This is intentionally separate from `FormModel`: SchemaForm edits values,
/// while this model authors the schema that later drives those forms.
public struct PipelineParameterDefinition: Codable, Hashable, Identifiable, Sendable {
    public var id: String { key }

    public var key: String
    public var kind: Param.Kind
    public var title: String
    public var description: String
    public var required: Bool
    public var stage: Param.Stage
    public var defaultValue: JSONValue?
    public var choices: [String]
    public var minimum: Double?
    public var maximum: Double?
    public var allowedSecrets: [String]

    public init(
        key: String,
        kind: Param.Kind = .string,
        title: String = "",
        description: String = "",
        required: Bool = false,
        stage: Param.Stage = .runtime,
        defaultValue: JSONValue? = nil,
        choices: [String] = [],
        minimum: Double? = nil,
        maximum: Double? = nil,
        allowedSecrets: [String] = []
    ) {
        self.key = key
        self.kind = kind
        self.title = title
        self.description = description
        self.required = required
        self.stage = stage
        self.defaultValue = defaultValue
        self.choices = choices
        self.minimum = minimum
        self.maximum = maximum
        self.allowedSecrets = allowedSecrets
    }

    public init(_ param: Param) {
        key = param.key
        kind = param.kind
        title = param.title
        description = param.description
        required = param.required
        stage = param.stage
        defaultValue = param.defaultValue
        choices = param.choices
        minimum = param.lo
        maximum = param.hi
        allowedSecrets = param.allow
    }

    public func parameter() throws -> Param {
        var descriptor: [String: JSONValue] = [
            "key": .string(key),
            "kind": .string(kind.rawValue),
            "title": .string(title.isEmpty ? key : title),
            "description": .string(description),
            "required": .bool(required),
            "advanced": .bool(false),
            "secret": .bool(false),
            "stage": .string(stage.rawValue),
            "enforce": .string("reject"),
        ]
        if let defaultValue { descriptor["default"] = defaultValue }
        if !choices.isEmpty { descriptor["choices"] = .array(choices.map(JSONValue.string)) }
        if let minimum { descriptor["lo"] = .double(minimum) }
        if let maximum { descriptor["hi"] = .double(maximum) }
        if !allowedSecrets.isEmpty {
            descriptor["allow"] = .array(allowedSecrets.map(JSONValue.string))
        }
        return try roundTrip(JSONValue.object(descriptor), as: Param.self)
    }
}

public enum NodeBindingMode: String, CaseIterable, Codable, Hashable, Sendable {
    case literal
    case pipelineParameter
    case secretReference
}

public struct NodeBindingOptions: Hashable, Sendable {
    public let parameterKeys: [String]
    public let secretNames: [String]

    public init(field: Param, parameters: [Param], configuredSecrets: [String]) {
        parameterKeys = parameters
            .filter { $0.kind == field.kind }
            .map(\.key)
            .sorted()
        let allowed = field.allow.isEmpty
            ? configuredSecrets
            : configuredSecrets.filter(Set(field.allow).contains)
        secretNames = allowed.sorted()
    }

    public func supports(_ mode: NodeBindingMode) -> Bool {
        switch mode {
        case .literal:
            true
        case .pipelineParameter:
            !parameterKeys.isEmpty
        case .secretReference:
            !secretNames.isEmpty
        }
    }
}

extension NodeConfigValue {
    public var bindingMode: NodeBindingMode {
        switch self {
        case .literal: .literal
        case .parameter: .pipelineParameter
        case .secret: .secretReference
        }
    }
}
