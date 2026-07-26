import Foundation
import Testing
@testable import WindexKit

@Suite("Registry cache")
struct RegistryCacheTests {
    @Test("stored ETag revalidates with 304 and reuses the registry")
    func notModifiedReusesRegistry() async throws {
        let etag = "\"sha256:registry-v1\""
        let body = """
        {
          "always_before_load": [],
          "contract": "windex.registry/3",
          "kinds": [],
          "modules": [],
          "port_types": {},
          "ports": [],
          "registry_contract": "windex.registry/3",
          "registry_digest": "sha256:registry-v1",
          "registry_version": 3
        }
        """
        let server = try MockWindexServer()
        server.on("GET /admin/v1/registry") { request in
            if request.header("If-None-Match") == etag {
                return .init(
                    status: 304,
                    headers: ["ETag": etag],
                    body: Data()
                )
            }
            return .init(
                headers: [
                    "Content-Type": "application/json",
                    "ETag": etag,
                ],
                body: Data(body.utf8)
            )
        }
        try await server.start()
        defer { server.stop() }

        let cacheURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("windex-registry-\(UUID().uuidString).json")
        defer { try? FileManager.default.removeItem(at: cacheURL) }
        let client = WindexClient(baseURL: server.baseURL, token: "token")
        let cache = RegistryCache(client: client, fileURL: cacheURL)

        let fresh = try await cache.load()
        let cached = try await cache.load()

        #expect(fresh.registryDigest == "sha256:registry-v1")
        #expect(cached == fresh)
        #expect(await !cache.wasStale)
        #expect(server.requests.count == 2)
        #expect(server.requests[0].header("If-None-Match") == nil)
        #expect(server.requests[1].header("If-None-Match") == etag)
    }
}
