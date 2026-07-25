import Foundation
import Testing
@testable import WindexKit
@testable import WindexUI

/// The rules that make `SchemaForm` correct, tested without rendering anything.
@MainActor
@Suite("FormModel")
struct FormModelTests {

    // MARK: Helpers

    private func param(_ json: String) throws -> Param {
        try JSONDecoder().decode(Param.self, from: Data(json.utf8))
    }

    /// A minimal param with overrides merged in.
    ///
    /// Merged rather than string-appended: a duplicate key in a JSON literal is
    /// silently resolved by the decoder, so appending `"advanced": true` to a
    /// template that already says `false` produces whichever the decoder happens
    /// to pick — a test that quietly measures the wrong thing.
    private func makeParam(key: String = "k", kind: String = "str",
                           extra: String = "") throws -> Param {
        var object: [String: Any] = [
            "key": key, "kind": kind, "choices": [], "label": key, "help": "",
            "type": "string", "title": key, "description": "", "required": false,
            "advanced": false, "secret": false, "stage": "runtime",
            "enforce": "clamp",
        ]
        if !extra.isEmpty {
            let overrides = try JSONSerialization.jsonObject(
                with: Data("{\(extra.drop(while: { $0 == "," }))}".utf8))
            for (k, v) in (overrides as? [String: Any]) ?? [:] { object[k] = v }
        }
        let data = try JSONSerialization.data(withJSONObject: object)
        return try JSONDecoder().decode(Param.self, from: data)
    }

    private func numeric(key: String = "n", lo: Double = 1, hi: Double = 10,
                         enforce: String = "clamp", unit: String? = nil,
                         kind: String = "int") throws -> Param {
        let unitPart = unit.map { ", \"unit\": \"\($0)\"" } ?? ""
        return try param("""
            {"key":"\(key)","kind":"\(kind)","lo":\(lo),"hi":\(hi),"choices":[],
             "label":"\(key)","help":"","type":"integer","editor":"number",
             "title":"\(key)","description":"","required":false,"advanced":false,
             "secret":false,"stage":"runtime","enforce":"\(enforce)",
             "clamp":"both"\(unitPart)}
            """)
    }

    // MARK: Seeding

    @Test("fields seed from prefill, then default")
    func seedsFromPrefillThenDefault() throws {
        let withPrefill = try makeParam(key: "a",
                                        extra: #", "default": "big", "prefill": "small""#)
        let withDefault = try makeParam(key: "b", extra: #", "default": "d""#)
        let bare = try makeParam(key: "c")

        let model = FormModel(params: [withPrefill, withDefault, bare])

        #expect(model.value(forKey: "a") == .string("small"))
        #expect(model.value(forKey: "b") == .string("d"))
        #expect(model.value(forKey: "c") == nil)
        #expect(!model.isDirty, "seeding is not an edit")
    }

    @Test("a settings scope seeds from its live values")
    func seedsFromScope() throws {
        let scope = try JSONDecoder().decode(SettingsScope.self, from: Data("""
            {"scope":"arxiv","fields":[
              {"key":"arxiv_request_interval","kind":"float","lo":3.0,"hi":60,
               "choices":[],"label":"Interval","help":"","type":"number",
               "editor":"number","title":"Interval","description":"",
               "required":false,"advanced":false,"secret":false,"stage":"runtime",
               "enforce":"clamp","clamp":"both","value":7.5,"origin":"db"}]}
            """.utf8))

        let model = FormModel(scope: scope)
        #expect(model.value(forKey: "arxiv_request_interval") == .double(7.5))
        #expect(!model.isDirty)
    }

    // MARK: Dirty tracking

    /// A PATCH merges server-side, so sending unchanged keys would convert every
    /// untouched default into an explicit override — the origin column would go
    /// all-`db` after one save.
    @Test("only changed keys are submitted")
    func onlyChangedKeysSubmit() throws {
        let a = try makeParam(key: "a", extra: #", "default": "one""#)
        let b = try makeParam(key: "b", extra: #", "default": "two""#)
        let model = FormModel(params: [a, b])

        #expect(model.changes.isEmpty)

        model.set("a", .string("changed"))
        #expect(model.isDirty)
        #expect(model.changes == ["a": .string("changed")])

        // Setting it back is not a change.
        model.set("a", .string("one"))
        #expect(!model.isDirty)
        #expect(model.changes.isEmpty)
    }

    @Test("reset discards edits and errors")
    func resetDiscards() throws {
        let p = try makeParam(key: "a", extra: #", "default": "one""#)
        let model = FormModel(params: [p])

        model.set("a", .string("edited"))
        model.apply([ValidationFailureFixture.make(field: "a", msg: "nope")])
        #expect(model.isDirty)

        model.reset()
        #expect(!model.isDirty)
        #expect(model.value(forKey: "a") == .string("one"))
        #expect(model.error(for: p) == nil)
    }

    // MARK: dependsOn

    /// §5.1: dim and disable, never hide. An operator looking for a setting they
    /// know exists must be able to see it and learn what it needs.
    @Test("an unmet dependency disables but never hides")
    func dependencyDisablesNotHides() throws {
        let mode = try makeParam(key: "mode", extra: #", "default": "off""#)
        let detail = try makeParam(
            key: "detail",
            extra: #", "dependsOn": {"field":"mode","equals":"on"}"#)
        let model = FormModel(params: [mode, detail])

        #expect(!model.isSatisfied(detail))
        #expect(!model.isEnabled(detail))
        // Still present in the rendered set — that is what "never hide" means.
        #expect(model.sections.flatMap(\.fields).contains { $0.key == "detail" })
        #expect(model.helperText(for: detail) == "Requires mode.")

        model.set("mode", .string("on"))
        #expect(model.isSatisfied(detail))
        #expect(model.isEnabled(detail))
    }

    /// A disabled control's value is not the operator's intent, so it must not
    /// ride along in the patch.
    @Test("a disabled field is excluded from the submission")
    func disabledFieldsAreNotSubmitted() throws {
        let mode = try makeParam(key: "mode", extra: #", "default": "off""#)
        let detail = try makeParam(
            key: "detail",
            extra: #", "dependsOn": {"field":"mode","equals":"on"}"#)
        let model = FormModel(params: [mode, detail])

        model.set("detail", .string("something"))
        #expect(model.changes["detail"] == nil, "gated off, so not intent")

        model.set("mode", .string("on"))
        #expect(model.changes["detail"] == .string("something"))
    }

    @Test("a locked field is disabled and explains itself")
    func lockedFieldExplains() throws {
        let locked = try makeParam(key: "k", extra: #", "lockedReason": "set in .env""#)
        let model = FormModel(params: [locked])

        #expect(!model.isEnabled(locked))
        #expect(model.helperText(for: locked) == "set in .env")
        #expect(model.sections.flatMap(\.fields).count == 1, "never hidden")
    }

    // MARK: Clamp vs reject

    /// The distinction that makes the form honest. A clamp param accepts
    /// out-of-range input and previews the adjustment; a reject param blocks it.
    @Test("clamp previews the adjustment instead of erroring")
    func clampPreviewsRatherThanErrors() throws {
        let p = try numeric(lo: 3, hi: 60, enforce: "clamp", kind: "float")
        let model = FormModel(params: [p])

        model.set("n", .double(0.5))
        #expect(model.error(for: p) == nil, "a clamp param must not block submit")
        #expect(model.clampPreview(for: p) == .double(3.0))
        #expect(model.canSubmit)

        model.set("n", .double(10))
        #expect(model.clampPreview(for: p) == nil, "in range, nothing to preview")
    }

    @Test("a reject param blocks submit and shows no clamp preview")
    func rejectBlocksSubmit() throws {
        let p = try numeric(lo: 1, hi: 365, enforce: "reject")
        let model = FormModel(params: [p])

        model.set("n", .int(9999))
        #expect(model.error(for: p) != nil)
        #expect(model.clampPreview(for: p) == nil,
                "clamping a reject param would submit what the operator did not type")
        #expect(!model.canSubmit)

        model.set("n", .int(30))
        #expect(model.error(for: p) == nil)
        #expect(model.canSubmit)
    }

    // MARK: Server response is the truth

    /// A clamped value comes back different from what was sent. A form that
    /// keeps its own input displays a lie.
    @Test("applying the response adopts the server's value and explains it")
    func responseIsAuthoritative() throws {
        let p = try numeric(key: "arxiv_request_interval", lo: 3, hi: 60,
                            enforce: "clamp", unit: "s", kind: "float")
        let model = FormModel(params: [p])
        model.set("arxiv_request_interval", .double(0.5))

        let returned = try JSONDecoder().decode(SettingsScope.self, from: Data("""
            {"scope":"arxiv","fields":[
              {"key":"arxiv_request_interval","kind":"float","lo":3.0,"hi":60,
               "choices":[],"label":"Interval","help":"","type":"number",
               "editor":"number","title":"Interval","description":"",
               "required":false,"advanced":false,"secret":false,"stage":"runtime",
               "enforce":"clamp","clamp":"both","unit":"s",
               "value":3.0,"origin":"db"}]}
            """.utf8))

        model.apply(returned.fields)

        #expect(model.value(forKey: "arxiv_request_interval") == .double(3.0))
        #expect(!model.isDirty, "the response is the new baseline")
        let notice = try #require(model.clampNotices["arxiv_request_interval"])
        #expect(notice.contains("3"))
        #expect(notice.contains("floor"), "say WHY it moved, not just that it did")
    }

    @Test("a ceiling adjustment is worded as a ceiling")
    func ceilingNoticeWording() throws {
        let p = try numeric(lo: 1, hi: 64, kind: "int")
        let notice = FormModel.clampNotice(for: p, submitted: .int(500), returned: .int(64))
        #expect(notice.contains("ceiling"))
        #expect(!notice.contains("floor"))
    }

    /// A secret is write-only — nothing comes back — so applying the response
    /// must not blank the control the operator just typed into.
    @Test("a secret keeps its typed value when the response omits it")
    func secretSurvivesApply() throws {
        let secret = try param("""
            {"key":"api_key","kind":"secret_ref","choices":[],"label":"Key","help":"",
             "type":"string","editor":"secret","title":"Key","description":"",
             "required":false,"advanced":false,"secret":true,"stage":"install",
             "enforce":"clamp"}
            """)
        let model = FormModel(params: [secret])
        model.set("api_key", .string("openai_api_key"))

        let returned = try JSONDecoder().decode([SettingsField].self, from: Data("""
            [{"key":"api_key","kind":"secret_ref","choices":[],"label":"Key",
              "help":"","type":"string","editor":"secret","title":"Key",
              "description":"","required":false,"advanced":false,"secret":true,
              "stage":"install","enforce":"clamp","origin":"db"}]
            """.utf8))

        model.apply(returned)
        #expect(model.value(forKey: "api_key") == .string("openai_api_key"))
    }

    // MARK: Server errors

    @Test("a 422 attaches its messages to the right controls")
    func validationErrorsAttachToFields() throws {
        let a = try makeParam(key: "a")
        let b = try makeParam(key: "b")
        let model = FormModel(params: [a, b])

        let error = WindexError.validation(
            failures: [ValidationFailureFixture.make(field: "b", msg: "must be a URL")],
            message: "rejected")
        let banner = model.apply(error)

        #expect(model.error(for: b) == "must be a URL")
        #expect(model.error(for: a) == nil)
        #expect(banner == nil, "an attributable failure needs no banner")
    }

    @Test("editing a field clears its server error")
    func editingClearsServerError() throws {
        let p = try makeParam(key: "a")
        let model = FormModel(params: [p])

        model.apply([ValidationFailureFixture.make(field: "a", msg: "nope")])
        #expect(model.error(for: p) == "nope")

        model.set("a", .string("fixed"))
        #expect(model.error(for: p) == nil,
                "a field just touched should not still show the last complaint")
    }

    @Test("a non-field error becomes a banner")
    func nonFieldErrorBecomesBanner() throws {
        let model = FormModel(params: [try makeParam()])
        let banner = model.apply(WindexError.http(status: 500, message: "boom"))
        #expect(banner == "boom")
    }

    // MARK: Grouping

    @Test("sections keep declaration order and split out advanced")
    func sectionsPreserveOrderAndSplitAdvanced() throws {
        let params = [
            try makeParam(key: "a", extra: #", "section": "Fetch""#),
            try makeParam(key: "b", extra: #", "section": "Fetch", "advanced": true"#),
            try makeParam(key: "c", extra: #", "section": "Extract""#),
            try makeParam(key: "d", extra: #", "section": "Fetch""#),
        ]
        let model = FormModel(params: params)
        let sections = model.sections

        #expect(sections.map(\.name) == ["Fetch", "Extract"])
        #expect(sections[0].fields.map(\.key) == ["a", "d"])
        #expect(sections[0].advanced.map(\.key) == ["b"])
        #expect(sections[1].fields.map(\.key) == ["c"])
    }

    @Test("a hidden editor is not rendered but still holds a value")
    func hiddenIsNotRendered() throws {
        let hidden = try makeParam(key: "h", extra: #", "editor": "hidden""#)
        let shown = try makeParam(key: "s")
        let model = FormModel(params: [hidden, shown])

        #expect(model.sections.flatMap(\.fields).map(\.key) == ["s"])
        model.set("h", .string("carried"))
        #expect(model.changes["h"] == .string("carried"))
    }

    // MARK: Real schema

    /// Against the actual 30 fields the server declares, not a fixture.
    @Test("the real global scope builds a form")
    func realScopeBuildsAForm() throws {
        let scope = try JSONDecoder().decode(SettingsScope.self, from: Data("""
            {"scope":"_global","fields":[
              {"key":"embed_concurrency","kind":"int","lo":1,"hi":64,"choices":[],
               "label":"Embed concurrency","help":"In-flight embed requests.",
               "type":"integer","editor":"number","title":"Embed concurrency",
               "description":"In-flight embed requests.","required":false,
               "advanced":false,"secret":false,"stage":"runtime","enforce":"clamp",
               "clamp":"both","value":12,"origin":"db"},
              {"key":"embed_order","kind":"choice","choices":["oldest","newest"],
               "label":"Embed order","help":"","type":"string","editor":"select",
               "title":"Embed order","description":"","required":false,
               "advanced":false,"secret":false,"stage":"runtime","enforce":"clamp",
               "value":"newest","origin":"env"}]}
            """.utf8))

        let model = FormModel(scope: scope)
        #expect(model.params.count == 2)
        #expect(model.value(forKey: "embed_concurrency") == .int(12))
        #expect(model.value(forKey: "embed_order") == .string("newest"))

        // A choice outside the declared set is an error even though the param
        // is nominally `clamp` — clamping only applies to numerics.
        model.set("embed_order", .string("sideways"))
        #expect(model.error(for: model.params[1]) != nil)
    }
}

/// `ValidationFailure` decodes from the server's JSON and has no memberwise
/// init, so tests build one the same way the client does.
enum ValidationFailureFixture {
    static func make(field: String, msg: String) -> ValidationFailure {
        let json = """
            {"loc":["body","\(field)"],"msg":"\(msg)","type":"value_error"}
            """
        // Force-try is acceptable in a fixture: a malformed literal here is a
        // test bug that should fail loudly at the first run.
        return try! JSONDecoder().decode(ValidationFailure.self, from: Data(json.utf8))
    }
}
