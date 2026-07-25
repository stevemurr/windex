import Foundation
import OpenAPIRuntime
import Testing
@testable import WindexKit

/// Guards the generation pipeline itself.
///
/// There is exactly one decoder per wire shape: the control plane's DTOs are
/// generated, `Param` is hand-written and serves all three places that shape
/// occurs. These tests protect the two invariants that keep it that way — the
/// nullable-union normalization, and the removal of the domain-owned schemas —
/// because both fail SILENTLY if lost. The generated code still compiles; it just
/// quietly stops carrying most of its fields, or reintroduces a second decoder.
@Suite("Generation pipeline")
struct GeneratedConformanceTests {

    private func decode<T: Decodable>(_ type: T.Type, _ json: String) throws -> T {
        try JSONDecoder().decode(T.self, from: Data(json.utf8))
    }

    /// `Tools/normalize_spec.py` collapses `anyOf: [X, null]` before generation
    /// because swift-openapi-generator drops such properties outright rather than
    /// erroring — it discarded 145 of 199 the first time this ran. Without it the
    /// Loops screen renders blank state columns and nothing reports a problem.
    @Test("nullable properties survived generation as optionals")
    func nullableUnionsBecameOptionals() throws {
        // LoopState keeps 5 of its 7 properties only through normalization.
        let loop = try decode(LoopState.self, """
            {"source":"ccnews","enabled":true,"running":true,"state":"up",
             "ingest_enabled":true,"log":"embed-loop","pids":[]}
            """)
        #expect(loop.enabled == true)
        #expect(loop.running == true)
        #expect(loop.state == "up")
        #expect(loop.ingestEnabled == true)
        #expect(loop.log == "embed-loop")

        // ScheduleEntry keeps 11 of 12.
        let entry = try decode(ScheduleEntry.self, """
            {"name":"daily","kind":"command","target":"daily","hour":2,"minute":15,
             "weekday":null,"enabled":true,"running":false,
             "last_run":"2026-07-24T02:15:52+00:00","last_run_ts":1784859352.5,
             "label":"Daily freshness","cadence":"daily · 02:15"}
            """)
        #expect(entry.hour == 2)
        #expect(entry.minute == 15)
        #expect(entry.enabled == true)
        #expect(entry.cadence == "daily · 02:15")
        #expect(entry.label == "Daily freshness")
    }

    /// Real payloads captured from a running backend, so the generated types are
    /// checked against what the server actually sends rather than against the
    /// spec's description of it.
    @Test("live-captured ops payloads decode")
    func livePayloadsDecode() throws {
        let loops = try decode(LoopsState.self, """
            {"watchdog_running":false,"indexing_paused":true,
             "loops":[{"source":"ccnews","enabled":true,"running":true,"state":"up",
                       "pids":[],"ingest_enabled":true,"log":"embed-loop"}]}
            """)
        #expect(loops.indexingPaused == true)
        #expect(loops.loops?.count == 1)

        let freshness = try decode([SourceFreshness].self, """
            [{"source":"wiki","indexed":2408435,"pending":4800208,
              "last_embed_ts":null,"last_update_ts":1784865055.0}]
            """)
        #expect(freshness.first?.indexed == 2408435)
        #expect(freshness.first?.pending == 4800208)

        let activity = try decode([ActivityItem].self, """
            [{"name":"refresh","label":"Refresh sweep","group":"action",
              "running":false,"last_ts":null,"error":false}]
            """)
        #expect(activity.first?.group == "action")

        let series = try decode([TimeseriesPoint].self, """
            [{"t":"2026-07-24T22:33:00+00:00","ingested":0,"docs":1584,"mb":0.0}]
            """)
        #expect(series.first?.docs == 1584)

        let logs = try decode([LogSource].self, """
            [{"name":"server","title":"Server","description":"REST API",
              "category":"server","kind":"file","size":19752,"mtime":1784935937,
              "available":true}]
            """)
        #expect(logs.first?.available == true)
        #expect(logs.first?.size == 19752)
    }

    /// `JobInfo.params` is untyped in the spec but carries `Param.describe()` on
    /// the wire — the same shape settings use. This is the reason `Param` is
    /// hand-written: a generated `SettingsField` could not serve this call site.
    @Test("job params decode into the same Param the settings form uses")
    func jobParamsAreParams() throws {
        // Captured from the live box's GET /v1/jobs.
        let job = try decode(JobInfo.self, """
            {"name":"ccnews-sync","title":"Find new shards",
             "description":"Check Common Crawl for new WARC files in the window",
             "category":"news","running":false,"pids":[],"confirm":false,
             "last_log":"",
             "params":{"days":{"key":"days","kind":"int","lo":1,"hi":365,
                               "choices":[],"label":"Days","help":"",
                               "type":"integer","editor":"number","title":"Days",
                               "description":"","required":false,"advanced":false,
                               "secret":false,"stage":"runtime",
                               "enforce":"reject","default":90}}}
            """)
        #expect(job.name == "ccnews-sync")

        let params = try job.parameters()
        let days = try #require(params["days"])
        #expect(days.kind == .int)
        #expect(days.editor == .number)
        #expect(days.lo == 1)
        #expect(days.hi == 365)
        #expect(days.defaultValue == .int(90))

        // Job params reject rather than clamp — the form must refuse out-of-range
        // input here, where the settings form would silently accept it.
        #expect(days.enforce == .reject)
        #expect(days.clamped(.int(9999)) == nil)
        #expect(days.validate(.int(9999)) != nil)
        #expect(days.validate(.int(30)) == nil)
    }

    /// The domain-owned schemas must NOT come back on regeneration. If they do,
    /// there are two decoders for the settings-field shape again and this file's
    /// whole premise is gone.
    ///
    /// Checked by grepping the generated source: a reintroduced type would
    /// otherwise be a silent duplicate that compiles perfectly well under a
    /// different namespace.
    @Test("no generated twin exists for the domain-owned shapes")
    func domainOwnedSchemasStayRemoved() throws {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()          // WindexKitTests
            .deletingLastPathComponent()          // Tests
            .deletingLastPathComponent()          // WindexKit
            .appendingPathComponent("Sources/WindexKit/Generated/Types.swift")

        let source = try String(contentsOf: url, encoding: .utf8)
        for name in ["SettingsField", "SettingsScope", "SettingsAll"] {
            #expect(!source.contains("struct \(name)"),
                    "\(name) was regenerated — normalize_spec.py's DOMAIN_OWNED removal was lost, so there are now two decoders for it")
        }
        // Sanity: the file is real and the ops types ARE there.
        #expect(source.contains("struct LoopState"))
    }
}
