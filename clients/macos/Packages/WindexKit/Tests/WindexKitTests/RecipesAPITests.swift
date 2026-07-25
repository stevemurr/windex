import Foundation
import Testing
@testable import WindexKit

@Suite("Recipes API")
struct RecipesAPITests {

    private func makeServer() throws -> MockWindexServer {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/recipes", json: """
            {"recipes":[
              {"name":"gh","source":"github","kind":"ingest",
               "title":"GitHub","description":"GitHub projects",
               "enabled":true,"builtin":true,"version":1},
              {"name":"wiki","source":"wiki","kind":"ingest",
               "title":"Wikipedia","enabled":true,"builtin":true,"version":1}
            ]}
            """)
        server.on("GET /admin/v1/recipes/gh", json: """
            {"name":"gh","source":"github","kind":"ingest","title":"GitHub",
             "enabled":true,"builtin":true,"version":1,
             "flows":{"discover":{
               "nodes":["events","repos","store"],
               "edges":[["events","store"],["repos","store"]]}},
             "spec":{"version":1,"name":"gh","refresh":["discover"],
               "flows":{"discover":{"nodes":{},"edges":[]}}}}
            """)
        server.on("GET /admin/v1/recipes/gh/tasks", json: """
            {"recipe":"gh","flow":"discover","tasks":[
              {"node":"events","kind":"discover","module":"github.events",
               "lane":"net","config":{"days":7},"depends_on":[],
               "preconditions":["network"],"weight":0.1,
               "max_attempts":3,"lease_seconds":900},
              {"node":"store","kind":"load","module":"documents.upsert",
               "lane":"io","config":{},"depends_on":["events","repos"],
               "preconditions":["postgres"],"weight":0.5,
               "max_attempts":3,"lease_seconds":300}
            ]}
            """)
        return server
    }

    @Test("the list is authenticated and omits full specs by default")
    func list() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let recipes = try await client.recipes()

        #expect(recipes.map(\.name) == ["gh", "wiki"])
        #expect(recipes[0].displayTitle == "GitHub")
        #expect(recipes[0].builtin == true)
        #expect(server.lastRequest?.path == "/admin/v1/recipes")
        #expect(server.lastRequest?.query["include_spec"] == nil)
        #expect(server.lastRequest?.header("Authorization") == "Bearer secret")
    }

    @Test("include-spec uses the server query name")
    func includeSpec() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        _ = try await client.recipes(includeSpec: true)

        #expect(server.lastRequest?.query["include_spec"] == "true")
    }

    @Test("one recipe exposes its graph and normalized document")
    func detail() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let recipe = try await client.recipe(named: "gh")
        let flows = try recipe.flowSummaries()
        let document = try recipe.document()

        #expect(recipe.source == "github")
        #expect(flows["discover"]?.nodes == ["events", "repos", "store"])
        #expect(flows["discover"]?.edges.count == 2)
        #expect(document?["name"] == .string("gh"))
        #expect(server.lastRequest?.path == "/admin/v1/recipes/gh")
    }

    @Test("task placement decodes and sends an optional flow")
    func tasks() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        let response = try await client.recipeTasks(named: "gh", flow: "discover")
        let tasks = try response.placements()

        #expect(response.recipe == "gh")
        #expect(tasks.count == 2)
        #expect(tasks[0].module == "github.events")
        #expect(tasks[0].lane == "net")
        #expect(tasks[0].config["days"] == .int(7))
        #expect(tasks[1].dependsOn == ["events", "repos"])
        #expect(tasks[1].leaseSeconds == 300)
        #expect(server.lastRequest?.query["flow"] == "discover")
    }

    @Test("an unknown recipe remains a typed not-found")
    func notFound() async throws {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/recipes/missing") { _ in
            .detail("unknown recipe: missing", status: 404)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "secret")
        do {
            _ = try await client.recipe(named: "missing")
            Issue.record("expected a throw")
        } catch let error as WindexError {
            guard case .notFound(let message) = error else {
                Issue.record("expected .notFound, got \(error)")
                return
            }
            #expect(message == "unknown recipe: missing")
        }
    }
}
