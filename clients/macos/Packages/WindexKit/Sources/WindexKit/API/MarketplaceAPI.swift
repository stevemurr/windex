import Foundation

private struct MarketplaceInstallRequest: Codable, Sendable {
    let name: String?
    let values: [String: JSONValue]
}

extension WindexClient {

    public func marketplace() async throws -> [MarketplaceEntry] {
        let page: MarketplaceList = try await send(
            "GET", "/v1/marketplace", surface: .admin,
            as: MarketplaceList.self)
        return page.entries ?? []
    }

    public func marketplaceEntry(id: String) async throws -> MarketplaceEntry {
        try await send(
            "GET", "/v1/marketplace/\(escape(id))", surface: .admin,
            as: MarketplaceEntry.self)
    }

    public func installMarketplaceEntry(
        id: String,
        name: String? = nil,
        values: [String: JSONValue] = [:]
    ) async throws -> Recipe {
        try await send(
            "POST", "/v1/marketplace/\(escape(id))/install",
            surface: .admin,
            body: MarketplaceInstallRequest(name: name, values: values),
            as: Recipe.self)
    }

    public func updateMarketplaceEntry(id: String) async throws -> Recipe {
        try await send(
            "POST", "/v1/marketplace/\(escape(id))/update",
            surface: .admin,
            as: Recipe.self)
    }
}
