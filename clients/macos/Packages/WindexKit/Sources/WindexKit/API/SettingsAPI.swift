import Foundation

// Settings live on the control plane, so every call here is `.admin` — i.e.
// `/admin/v1/settings/...` on the wire.
extension WindexClient {

    /// `GET /admin/v1/settings` — every scope's schema, value and origin.
    public func allSettings() async throws -> AllSettings {
        try await send("GET", "/v1/settings", surface: .admin, as: AllSettings.self)
    }

    /// `GET /admin/v1/settings/{scope}`.
    public func settings(scope: String) async throws -> SettingsScope {
        try await send("GET", "/v1/settings/\(escape(scope))", surface: .admin,
                       as: SettingsScope.self)
    }

    /// `PATCH /admin/v1/settings/{scope}` — set one or more overrides.
    ///
    /// All-or-nothing on the server: one bad key rejects the batch, so a form
    /// submit never lands half-applied. Numbers on a `clamp` param are pulled to
    /// their bound rather than rejected, which means **the response is the truth**
    /// — re-read the returned fields rather than assuming the submitted values
    /// took, or the form will show 0.5 where the server stored 1.0.
    @discardableResult
    public func patchSettings(scope: String,
                              values: [String: JSONValue]) async throws -> SettingsScope {
        try await send("PATCH", "/v1/settings/\(escape(scope))", surface: .admin,
                       body: SettingsPatch(values: values), as: SettingsScope.self)
    }

    /// `DELETE /admin/v1/settings/{scope}/{key}` — drop one override so the key
    /// falls back to env, then the code default. Returns the refreshed scope.
    @discardableResult
    public func revertSetting(scope: String, key: String) async throws -> SettingsScope {
        try await send("DELETE", "/v1/settings/\(escape(scope))/\(escape(key))",
                       surface: .admin, as: SettingsScope.self)
    }
}
