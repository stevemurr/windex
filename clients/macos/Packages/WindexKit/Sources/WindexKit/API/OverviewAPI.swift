import Foundation

extension WindexClient {
    public func overview() async throws -> OverviewWire {
        try await send("GET", "/v1/overview", surface: .admin, as: OverviewWire.self)
    }
}
