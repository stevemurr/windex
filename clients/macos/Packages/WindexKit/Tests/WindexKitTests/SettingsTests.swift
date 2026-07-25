import Foundation
import Testing
@testable import WindexKit

/// These run against fixtures GENERATED from windex's own `settings_schema`
/// (see `Fixtures/generate_fixtures.py`), so what's asserted here is the real
/// contract rather than a convenient invention.
@Suite("Settings + SchemaForm")
struct SettingsTests {

    private func makeServer() throws -> MockWindexServer {
        let server = try MockWindexServer()
        server.on("GET /admin/v1/settings") { _ in .json(Fixtures.allSettings) }
        for scope in Fixtures.settingsKeys.keys {
            server.on("GET /admin/v1/settings/\(scope)") { _ in
                .json(Fixtures.settingsScope(scope))
            }
        }
        return server
    }

    @Test("every scope and field the server declares decodes")
    func decodesRealSchema() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let all = try await client.allSettings()

        let expected = Fixtures.settingsKeys
        #expect(!expected.isEmpty, "fixtures failed to load")
        #expect(Set(all.scopes.map(\.scope)) == Set(expected.keys))

        for scope in all.scopes {
            let got = scope.fields.map(\.key).sorted()
            #expect(got == expected[scope.scope],
                    "scope \(scope.scope) fields drifted from the server schema")
        }
        // No param may decode to an unknown kind — that would mean the Swift
        // Kind enum has fallen behind param.py's KINDS.
        for scope in all.scopes {
            for field in scope.fields {
                if case .unknown(let raw) = field.param.kind {
                    Issue.record("\(field.key): unmodelled kind '\(raw)'")
                }
            }
        }
    }

    /// The clamp contract, on a real param: `arxiv_request_interval` has a floor
    /// of 3.0 because that IS arXiv's published ToU rate.
    @Test("a clamped numeric reports its bounds and both clamp ends")
    func clampedNumericCarriesBounds() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let arxiv = try await client.settings(scope: "arxiv")
        let interval = try #require(arxiv.fields.first { $0.key == "arxiv_request_interval" })

        #expect(interval.param.kind == .float)
        #expect(interval.param.editor == .number)
        #expect(interval.param.lo == 3.0)
        #expect(interval.param.hi == 60)
        #expect(interval.param.enforce == .clamp)
        #expect(interval.param.clamp == .both)

        // The form previews the adjustment the server would make.
        #expect(interval.param.clamped(.double(0.5)) == .double(3.0))
        #expect(interval.param.clamped(.double(999)) == .double(60))
        #expect(interval.param.clamped(.double(10)) == nil)   // in range, no change

        // And an out-of-range value is NOT a validation error under clamp — the
        // server accepts it and quietly honours the bound.
        #expect(interval.param.validate(.double(0.5)) == nil)
    }

    /// Under `reject` the server refuses instead of clamping, so the client must
    /// not "helpfully" adjust — it would submit something other than what was
    /// typed. This is the distinction `enforce` exists to carry.
    @Test("a reject param errors instead of clamping")
    func rejectParamDoesNotClamp() throws {
        let param = try decodeParam("""
            {"key":"max_pages","kind":"int","lo":1,"hi":100,"choices":[],
             "label":"Pages","help":"","type":"integer","editor":"number",
             "title":"Pages","description":"","required":false,"advanced":false,
             "secret":false,"stage":"runtime","enforce":"reject"}
            """)

        #expect(param.enforce == .reject)
        #expect(param.clamp == nil)
        #expect(param.clamped(.int(500)) == nil, "reject params must never be clamped")
        #expect(param.validate(.int(500)) != nil)
        #expect(param.validate(.int(50)) == nil)
    }

    /// An unrecognised `enforce` must fail safe. Defaulting to clamp would let
    /// the client adjust a value the server then refuses; defaulting to reject
    /// only costs a visible error.
    @Test("an unknown enforce value defaults to reject")
    func unknownEnforceFailsSafe() throws {
        let param = try decodeParam("""
            {"key":"k","kind":"int","lo":1,"hi":10,"choices":[],"label":"k","help":"",
             "type":"integer","editor":"number","title":"k","description":"",
             "required":false,"advanced":false,"secret":false,"stage":"runtime",
             "enforce":"something_new"}
            """)
        #expect(param.enforce == .reject)
        #expect(param.clamped(.int(99)) == nil)
    }

    /// A newer server adding a kind should degrade one control to a text field,
    /// not break the whole form.
    @Test("an unknown kind degrades to a text field")
    func unknownKindDegrades() throws {
        let param = try decodeParam("""
            {"key":"k","kind":"colour","choices":[],"label":"k","help":"",
             "type":"string","title":"k","description":"","required":false,
             "advanced":false,"secret":false,"stage":"runtime","enforce":"clamp"}
            """)
        #expect(param.kind == .unknown("colour"))
        #expect(param.editor == .textfield)
        #expect(param.validate(.string("x")) != nil)   // but not submittable
    }

    /// `csv` is stored as the raw comma string, not a list — rendering it as a
    /// list editor is a client affordance, and treating it as an array on the
    /// wire would corrupt the value.
    @Test("csv is a string on the wire but a list editor in the UI")
    func csvIsAStringWithAListEditor() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let docs = try await client.settings(scope: "docs")
        let slugs = try #require(docs.fields.first { $0.key == "docs_slugs" })

        #expect(slugs.param.kind == .csv)
        #expect(slugs.param.editor == .stringList)
        #expect(slugs.param.validate(.string("python,react")) == nil)
        #expect(slugs.param.validate(.array(["python", "react"])) != nil)
    }

    @Test("a choice param exposes its options")
    func choiceParam() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let global = try await client.settings(scope: SettingsScope.global)
        let order = try #require(global.fields.first { $0.key == "embed_order" })

        #expect(order.param.kind == .choice)
        #expect(order.param.editor == .select)
        #expect(order.param.choices == ["oldest", "newest"])
        #expect(order.param.validate(.string("newest")) == nil)
        #expect(order.param.validate(.string("sideways")) != nil)
        // No enumTitles declared, so a choice labels itself.
        #expect(order.param.title(forChoice: "newest") == "newest")
    }

    /// Only a `db` value has an override to drop. Offering revert on an env or
    /// default row would be a no-op button.
    @Test("origin distinguishes an override from a default")
    func originDrivesRevertAffordance() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let global = try await client.settings(scope: SettingsScope.global)

        let overridden = try #require(global.fields.first { $0.key == "embed_concurrency" })
        #expect(overridden.origin == .db)
        #expect(overridden.origin?.isOverride == true)
        #expect(overridden.value?.intValue == 12)

        let fromEnv = try #require(global.fields.first { $0.key == "embed_order" })
        #expect(fromEnv.origin == .env)
        #expect(fromEnv.origin?.isOverride == false)
    }

    /// Fields are grouped by `section` for rendering, and declaration order is
    /// meaningful — related knobs are adjacent in the schema.
    @Test("sectioning preserves the server's field order")
    func sectionsPreserveOrder() throws {
        let scope = try JSONDecoder().decode(
            SettingsScope.self,
            from: Data(Fixtures.settingsScope("smallweb").utf8))

        let flattened = scope.sections.flatMap { $0.fields.map(\.key) }
        #expect(flattened == scope.fields.map(\.key))
        #expect(scope.fields.first?.key == "smallweb_host_interval")
    }

    /// A PATCH is all-or-nothing and the RESPONSE is the truth — a clamped value
    /// comes back different from what was sent, and a form that assumes its own
    /// input took will display a lie.
    @Test("patch sends the values object and re-reads the server's result")
    func patchRoundTrip() async throws {
        let server = try makeServer()
        server.on("PATCH /admin/v1/settings/arxiv") { _ in
            // Server clamped 0.5 up to the 3.0 ToU floor.
            .json("""
                {"scope":"arxiv","fields":[
                  {"key":"arxiv_request_interval","kind":"float","lo":3.0,"hi":60,
                   "choices":[],"label":"Request interval (s)","help":"",
                   "type":"number","editor":"number","title":"Request interval (s)",
                   "description":"","required":false,"advanced":false,"secret":false,
                   "stage":"runtime","enforce":"clamp","clamp":"both",
                   "value":3.0,"origin":"db"}]}
                """)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        let updated = try await client.patchSettings(
            scope: "arxiv", values: ["arxiv_request_interval": .double(0.5)])

        let sent = try #require(server.lastRequest)
        #expect(sent.method == "PATCH")
        #expect(sent.header("Authorization") == "Bearer t")
        #expect(sent.header("Content-Type") == "application/json")
        #expect(sent.body.contains("\"values\""))
        #expect(sent.body.contains("arxiv_request_interval"))

        // The response, not the request, is what the form must show.
        #expect(updated.fields.first?.value?.doubleValue == 3.0)
        #expect(updated.fields.first?.origin == .db)
    }

    @Test("revert deletes the override and returns the refreshed scope")
    func revertSetting() async throws {
        let server = try makeServer()
        server.on("DELETE /admin/v1/settings/_global/embed_concurrency") { _ in
            .json(Fixtures.settingsScope("_global"))
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        _ = try await client.revertSetting(scope: SettingsScope.global,
                                           key: "embed_concurrency")

        let sent = try #require(server.lastRequest)
        #expect(sent.method == "DELETE")
        #expect(sent.path == "/admin/v1/settings/_global/embed_concurrency")
        #expect(sent.header("Authorization") == "Bearer t")
    }

    /// A 422 from a settings PATCH must attach to the offending key, which is
    /// what lets the form show the message on the control rather than as a toast.
    @Test("a rejected patch names the offending key")
    func patchValidationNamesTheKey() async throws {
        let server = try makeServer()
        server.on("PATCH /admin/v1/settings/_global") { _ in
            .detail("setting is not editable: 'embed_model'", status: 422)
        }
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "t")
        do {
            _ = try await client.patchSettings(scope: SettingsScope.global,
                                               values: ["embed_model": .string("x")])
            Issue.record("expected a throw")
        } catch let error as WindexError {
            // A string `detail` (not a list) is FastAPI's HTTPException shape and
            // maps to .http(422), still carrying the server's wording.
            #expect(error.localizedDescription.contains("embed_model"))
        }
    }

    /// Every settings call is on the control plane, so it must carry the token
    /// AND the /admin mount prefix. A missing prefix 404s against the real server.
    @Test("settings calls are sent to /admin with a bearer token")
    func settingsUseTheAdminSurface() async throws {
        let server = try makeServer()
        try await server.start()
        defer { server.stop() }

        let client = WindexClient(baseURL: server.baseURL, token: "s3cret")
        _ = try await client.allSettings()

        let sent = try #require(server.lastRequest)
        #expect(sent.path == "/admin/v1/settings")
        #expect(sent.header("Authorization") == "Bearer s3cret")
    }

    // MARK: - Helpers

    private func decodeParam(_ json: String) throws -> Param {
        try JSONDecoder().decode(Param.self, from: Data(json.utf8))
    }
}

/// Rules that are pure `Param` logic — the renderer's behaviour, no server needed.
@Suite("SchemaForm rules")
struct SchemaFormTests {

    private func decodeParam(_ json: String) throws -> Param {
        try JSONDecoder().decode(Param.self, from: Data(json.utf8))
    }

    private func param(_ extra: String) throws -> Param {
        try decodeParam("""
            {"key":"k","kind":"str","choices":[],"label":"k","help":"","type":"string",
             "editor":"textfield","title":"k","description":"","required":false,
             "advanced":false,"secret":false,"stage":"runtime","enforce":"clamp"\(extra)}
            """)
    }

    @Test("dependsOn equals shows and hides a control")
    func dependsOnEquals() throws {
        let p = try param(#", "dependsOn": {"field":"auth","equals":"token"}"#)
        let rule = try #require(p.dependsOn)

        #expect(rule.field == "auth")
        #expect(rule.isSatisfied(by: .string("token")))
        #expect(!rule.isSatisfied(by: .string("none")))
        #expect(!rule.isSatisfied(by: nil))
    }

    @Test("dependsOn in matches any listed value")
    func dependsOnIn() throws {
        let p = try param(#", "dependsOn": {"field":"mode","in":["a","b"]}"#)
        let rule = try #require(p.dependsOn)

        #expect(rule.isSatisfied(by: .string("a")))
        #expect(rule.isSatisfied(by: .string("b")))
        #expect(!rule.isSatisfied(by: .string("c")))
    }

    /// A rule the client can't understand must not hide a control the operator
    /// then can't find.
    @Test("an unparseable dependsOn shows the control")
    func unparseableDependsOnIsPermissive() throws {
        let p = try param(#", "dependsOn": {"field":"x"}"#)
        #expect(try #require(p.dependsOn).isSatisfied(by: nil))
    }

    @Test("lockedReason disables the control and explains why")
    func lockedParam() throws {
        let p = try param(#", "lockedReason": "set in .env""#)
        #expect(!p.isEditable)
        #expect(p.lockedReason == "set in .env")
        #expect(p.validate(.string("anything")) != nil)
    }

    /// A secret is write-only, so no value comes back on read and the form must
    /// fall back rather than render an empty required field.
    @Test("a secret field has no echoed value")
    func secretHasNoValue() throws {
        let field = try JSONDecoder().decode(SettingsField.self, from: Data("""
            {"key":"api_key","kind":"secret_ref","choices":[],"label":"Key","help":"",
             "type":"string","editor":"secret","title":"Key","description":"",
             "required":true,"advanced":false,"secret":true,"stage":"install",
             "enforce":"clamp","allow":["openai_api_key"],"origin":"db"}
            """.utf8))

        #expect(field.param.secret)
        #expect(field.value == nil)
        #expect(field.param.stage == .install)
        #expect(field.param.validate(.string("openai_api_key")) == nil)
        #expect(field.param.validate(.string("pg_dsn")) != nil,
                "a secret_ref must only name a key from its allowlist")
    }

    @Test("prefill wins over default as the form's starting value")
    func prefillWinsOverDefault() throws {
        let p = try param(#", "default": "everything", "prefill": "a,short,list""#)
        #expect(p.initialValue == .string("a,short,list"))
        #expect(p.defaultValue == .string("everything"))
    }

    @Test("enumTitles label choices when present")
    func enumTitlesLabelChoices() throws {
        let p = try decodeParam("""
            {"key":"order","kind":"choice","choices":["oldest","newest"],
             "enumTitles":["Drain backlog","Fresh first"],"label":"Order","help":"",
             "type":"string","editor":"select","title":"Order","description":"",
             "required":false,"advanced":false,"secret":false,"stage":"runtime",
             "enforce":"clamp"}
            """)
        #expect(p.title(forChoice: "oldest") == "Drain backlog")
        #expect(p.title(forChoice: "newest") == "Fresh first")
        #expect(p.title(forChoice: "unknown") == "unknown")
    }

    @Test("regex_list rejects an uncompilable pattern before submit")
    func regexListValidatesPatterns() throws {
        let p = try decodeParam("""
            {"key":"deny","kind":"regex_list","choices":[],"label":"Deny","help":"",
             "type":"array","editor":"regexList","title":"Deny","description":"",
             "required":false,"advanced":false,"secret":false,"stage":"runtime",
             "enforce":"clamp","maxItems":2}
            """)
        #expect(p.validate(.array([.string("^ok$")])) == nil)
        #expect(p.validate(.array([.string("[unclosed")])) != nil)
        #expect(p.validate(.array([.string("a"), .string("b"), .string("c")])) != nil)
        #expect(p.validate(.array([.string("")])) != nil)
    }

    /// Python's `coerce` rejects a bool for a numeric even though bool is an int
    /// there. The two sides must agree on what's valid, or the form accepts
    /// something the server 422s.
    @Test("a bool is not a number")
    func boolIsNotANumber() throws {
        let p = try decodeParam("""
            {"key":"n","kind":"int","lo":0,"hi":10,"choices":[],"label":"n","help":"",
             "type":"integer","editor":"number","title":"n","description":"",
             "required":false,"advanced":false,"secret":false,"stage":"runtime",
             "enforce":"reject"}
            """)
        #expect(p.validate(.bool(true)) != nil)
        #expect(p.validate(.int(5)) == nil)
        #expect(p.validate(.double(5.5)) != nil, "an int param rejects a fraction")
    }

    /// JSON has one number type, so a `float` param whose value happens to be
    /// `3.0` decodes as `.int(3)` — Int is tried first so integers round-trip as
    /// integers. Equality therefore has to be numeric, or a form comparing what
    /// it submitted against what came back sees a change that never happened and
    /// shows a false "Adjusted to 3 — the operator's floor".
    @Test("int and double forms of the same number are equal")
    func numericEqualityCrossesCases() throws {
        #expect(JSONValue.int(3) == JSONValue.double(3.0))
        #expect(JSONValue.double(3.0) == JSONValue.int(3))
        #expect(JSONValue.int(3) != JSONValue.double(3.5))
        #expect(JSONValue.int(3) != JSONValue.string("3"))
        #expect(JSONValue.bool(true) != JSONValue.int(1), "a bool is not a number")

        // Equal values must hash equally, or a Set/Dictionary disagrees with ==.
        #expect(Set([JSONValue.int(3), .double(3.0)]).count == 1)

        // The decode path that produces the mismatch in the first place.
        let decoded = try JSONDecoder().decode(JSONValue.self, from: Data("3.0".utf8))
        #expect(decoded == .double(3.0))
        #expect(decoded == .int(3))

        // Nested, since form values are compared inside dictionaries.
        #expect(JSONValue.object(["v": .int(3)]) == .object(["v": .double(3.0)]))
        #expect(JSONValue.array([.int(3)]) == .array([.double(3.0)]))
    }

    /// Round-tripping must not turn an int into a float: `3` re-encoded as `3.0`
    /// is a different value to a server reading it as an int.
    @Test("integers survive a JSON round trip as integers")
    func intsRoundTripAsInts() throws {
        let encoded = try JSONEncoder().encode(SettingsPatch(values: ["n": .int(3)]))
        #expect(String(decoding: encoded, as: UTF8.self).contains("\"n\":3"))

        let decoded = try JSONDecoder().decode(JSONValue.self, from: Data("3".utf8))
        #expect(decoded == .int(3))
    }
}
