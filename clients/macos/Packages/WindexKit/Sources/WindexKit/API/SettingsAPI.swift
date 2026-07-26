import Foundation

private struct Epoch2SettingsPatch: Codable, Sendable {
    let values: [String: JSONValue]
}

extension WindexClient {
    public func globalSettings() async throws -> GlobalSettingsWire {
        try await send("GET", "/v1/settings", surface: .admin,
                       as: GlobalSettingsWire.self)
    }

    @discardableResult
    public func patchGlobalSettings(_ values: [String: JSONValue], etag: String) async throws
        -> GlobalSettingsWire {
        try await send("PATCH", "/v1/settings", surface: .admin,
                       body: Epoch2SettingsPatch(values: values),
                       headers: ["If-Match": etag], as: GlobalSettingsWire.self)
    }

    @discardableResult
    public func deleteGlobalSetting(_ key: String, etag: String) async throws
        -> GlobalSettingsWire {
        try await send("DELETE", "/v1/settings/\(Self.escapePath(key))",
                       surface: .admin, headers: ["If-Match": etag],
                       as: GlobalSettingsWire.self)
    }

    public func secrets() async throws -> SecretsWire {
        try await send("GET", "/v1/secrets", surface: .admin, as: SecretsWire.self)
    }
}
