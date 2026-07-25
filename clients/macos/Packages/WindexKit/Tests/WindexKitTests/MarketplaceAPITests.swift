import Testing
@testable import WindexKit

@Suite("Marketplace API")
struct MarketplaceAPITests {

    @Test("catalog entries expose install schema and availability")
    func list() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/marketplace", json: """
            {"entries":[{
              "id":"windex:web_docs","catalog":"windex","name":"web_docs",
              "title":"Documentation website","description":"Crawl docs",
              "version":1,"document":{"name":"web_docs"},
              "config":[{"key":"seeds","kind":"url_list","required":true,
                         "label":"Seed URLs"}],
              "installed":false,"executable":false,
              "unavailable_modules":["http.get"]
            }]}
            """)
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let entries = try await client.marketplace()
        let params = try entries[0].installParameters()

        #expect(entries[0].id == "windex:web_docs")
        #expect(entries[0].executable == false)
        #expect(params.map(\.key) == ["seeds"])
        #expect(params[0].required)
        #expect(server.lastRequest?.header("Authorization") == "Bearer secret")
    }

    @Test("install sends identity and typed values")
    func install() async throws {
        let server = try MockWindexServer()
        server.on("POST /admin/v1/marketplace/windex:web_docs/install") {
            request in
            #expect(request.body.contains("\"name\":\"team_docs\""))
            #expect(request.body.contains("https:\\/\\/example.com\\/docs\\/"))
            return .json("""
                {"name":"team_docs","source":"team_docs","version":1,
                 "title":"Documentation website","builtin":false,
                 "enabled":true}
                """, status: 201)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let recipe = try await client.installMarketplaceEntry(
            id: "windex:web_docs",
            name: "team_docs",
            values: ["seeds": .array([.string("https://example.com/docs/")])])

        #expect(recipe.name == "team_docs")
    }
}
