import SwiftUI
import Testing
@testable import WindexKit
@testable import WindexUI

/// Rendering smoke tests.
///
/// A SwiftUI view that compiles is not a view that draws — a bad binding, a
/// missing environment value or a malformed `ForEach` id all fail at render
/// time. These force each editor through an actual layout pass by rendering to
/// an image, which is the cheapest way to catch that without a UI test host.
@MainActor
@Suite("SchemaForm rendering")
struct SchemaFormRenderTests {

    /// Force a real layout + draw. Returns nil if the view can't be rendered.
    private func render(_ view: some View, size: CGSize = .init(width: 480, height: 900))
        -> NSImage? {
        let renderer = ImageRenderer(
            content: view
                .frame(width: size.width)
                .windexTheme(.dark)
                .padding()
        )
        renderer.scale = 1
        return renderer.nsImage
    }

    private func model(_ json: String) throws -> FormModel {
        let scope = try JSONDecoder().decode(SettingsScope.self, from: Data(json.utf8))
        return FormModel(scope: scope)
    }

    /// One param per editor the design specifies a control for, rendered at once.
    @Test("every editor renders")
    func everyEditorRenders() throws {
        let editors: [(String, String, String)] = [
            // key, kind, editor
            ("a_text", "str", "textfield"),
            ("a_url", "url", "url"),
            ("a_area", "str", "textarea"),
            ("a_num", "int", "number"),
            ("a_bool", "bool", "checkbox"),
            ("a_select", "choice", "select"),
            ("a_multi", "csv", "multiselect"),
            ("a_list", "url_list", "stringList"),
            ("a_regex", "regex_list", "regexList"),
            ("a_kv", "str", "keyValue"),
            ("a_json", "str", "json"),
            ("a_date", "date", "datepicker"),
            ("a_dur", "duration", "duration"),
            ("a_secret", "secret_ref", "secret"),
            ("a_hidden", "str", "hidden"),
        ]
        let fields = editors.map { key, kind, editor in
            """
            {"key":"\(key)","kind":"\(kind)","choices":["one","two"],
             "label":"\(key)","help":"help text","type":"string",
             "editor":"\(editor)","title":"\(key)","description":"help text",
             "required":false,"advanced":false,"secret":\(editor == "secret"),
             "stage":"runtime","enforce":"clamp"}
            """
        }.joined(separator: ",")

        let form = try model("{\"scope\":\"t\",\"fields\":[\(fields)]}")
        #expect(form.params.count == editors.count)

        let image = render(SchemaForm(model: form), size: .init(width: 480, height: 2400))
        #expect(image != nil, "the form failed to render")
        #expect((image?.size.width ?? 0) > 0)
    }

    /// The three attributes §5.1 says must be rendered rather than dropped.
    @Test("locked, dependent and clamped fields render")
    func behaviouralAttributesRender() throws {
        let form = try model("""
            {"scope":"t","fields":[
              {"key":"mode","kind":"choice","choices":["on","off"],"label":"Mode",
               "help":"","type":"string","editor":"select","title":"Mode",
               "description":"","required":false,"advanced":false,"secret":false,
               "stage":"runtime","enforce":"clamp","value":"off","origin":"default"},
              {"key":"detail","kind":"str","choices":[],"label":"Detail","help":"",
               "type":"string","editor":"textfield","title":"Detail",
               "description":"","required":false,"advanced":false,"secret":false,
               "stage":"runtime","enforce":"clamp",
               "dependsOn":{"field":"mode","equals":"on"}},
              {"key":"locked","kind":"str","choices":[],"label":"Locked","help":"",
               "type":"string","editor":"textfield","title":"Locked",
               "description":"","required":false,"advanced":false,"secret":false,
               "stage":"runtime","enforce":"clamp","lockedReason":"set in .env"},
              {"key":"interval","kind":"float","lo":3.0,"hi":60,"choices":[],
               "label":"Interval","help":"","type":"number","editor":"number",
               "title":"Interval","description":"","required":false,
               "advanced":false,"secret":false,"stage":"runtime","enforce":"clamp",
               "clamp":"both","clampNote":"arXiv's published rate.","unit":"s"}]}
            """)

        // Put the clamp param out of range so the preview line renders too.
        form.set("interval", .double(0.5))
        #expect(form.clampPreview(for: form.params[3]) == .double(3.0))

        #expect(render(SchemaForm(model: form)) != nil)

        // And with the dependency satisfied, so the enabled branch draws.
        form.set("mode", .string("on"))
        #expect(render(SchemaForm(model: form)) != nil)
    }

    @Test("sections and the advanced disclosure render")
    func sectionsRender() throws {
        let form = try model("""
            {"scope":"t","fields":[
              {"key":"a","kind":"str","choices":[],"label":"A","help":"",
               "type":"string","editor":"textfield","title":"A","description":"",
               "required":true,"advanced":false,"secret":false,"stage":"runtime",
               "enforce":"clamp","section":"Fetch"},
              {"key":"b","kind":"int","lo":1,"hi":9,"choices":[],"label":"B",
               "help":"","type":"integer","editor":"number","title":"B",
               "description":"","required":false,"advanced":true,"secret":false,
               "stage":"runtime","enforce":"clamp","section":"Fetch"},
              {"key":"c","kind":"bool","choices":[],"label":"C","help":"",
               "type":"boolean","editor":"checkbox","title":"C","description":"",
               "required":false,"advanced":false,"secret":false,"stage":"runtime",
               "enforce":"clamp","section":"Extract"}]}
            """)

        #expect(form.sections.count == 2)
        #expect(render(SchemaForm(model: form)) != nil)
    }

    /// The real 30-field schema, all nine scopes, rendered.
    @Test("the real settings schema renders")
    func realSchemaRenders() throws {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()          // WindexUITests
            .deletingLastPathComponent()          // Tests
            .appendingPathComponent("WindexKitTests/Fixtures/settings.json")
        let data = try Data(contentsOf: url)
        let all = try JSONDecoder().decode(AllSettings.self, from: data)

        #expect(all.scopes.count == 9)
        var rendered = 0
        for scope in all.scopes {
            let form = FormModel(scope: scope)
            #expect(render(SchemaForm(model: form)) != nil,
                    "scope \(scope.scope) failed to render")
            rendered += form.params.count
        }
        #expect(rendered == 30, "expected the schema's 30 fields")
    }

    @Test("status badges render in every state")
    func statusRenders() {
        let stack = VStack {
            ForEach(Status.allCases, id: \.self) { status in
                StatusBadge(status)
            }
            StatusBadge(.attention, word: "paused")
            FeedStatus("skip", .healthy)
            FeedStatus("fail", .fault)
        }
        #expect(render(stack) != nil)
    }
}
