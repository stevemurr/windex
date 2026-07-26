import Foundation

extension WindexClient {
    public func overview() async throws -> OverviewWire {
        try await send("GET", "/v1/overview", surface: .admin, as: OverviewWire.self)
    }

    public func moduleHealth() async throws -> ModuleHealthWire {
        try await send(
            "GET",
            "/v1/module-health",
            surface: .admin,
            as: ModuleHealthWire.self
        )
    }
}
