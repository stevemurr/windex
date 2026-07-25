import Foundation
import Observation
import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class RecipesModel {
    private(set) var recipes: [Recipe] = []
    private(set) var validation: ValidationReport?
    private(set) var validationMessages: [String] = []
    private(set) var isLoading = false
    private(set) var isSaving = false
    private(set) var errorMessage: String?
    var selectedName: String?
    var documentText = ""
    var isNew = false

    var canSave: Bool {
        validation?.valid == true && !isSaving
    }

    func load(client: WindexClient, appModel: AppModel) async {
        isLoading = recipes.isEmpty
        do {
            recipes = try await client.recipes()
            isLoading = false
            errorMessage = nil
            if let selectedName,
               recipes.contains(where: { $0.name == selectedName }) {
                await select(selectedName, client: client, appModel: appModel)
            } else if let first = recipes.first {
                await select(first.name, client: client, appModel: appModel)
            }
        } catch {
            isLoading = false
            present(error, appModel: appModel)
        }
    }

    func select(_ name: String, client: WindexClient, appModel: AppModel) async {
        selectedName = name
        isNew = false
        validation = nil
        validationMessages = []
        do {
            let recipe = try await client.recipe(named: name)
            guard selectedName == name else { return }
            guard let document = try recipe.document() else {
                throw WindexError.decoding(
                    underlying: RecipeEditorError.missingDocument)
            }
            documentText = try pretty(.object(document))
            await validate(client: client, appModel: appModel)
        } catch {
            present(error, appModel: appModel)
        }
    }

    func newRecipe() {
        selectedName = nil
        isNew = true
        validation = nil
        validationMessages = []
        documentText = Self.template
    }

    func validate(client: WindexClient, appModel: AppModel) async {
        guard !documentText.isEmpty else { return }
        do {
            let document = try decodeDocument()
            let report = try await client.validateRecipe(document)
            validation = report
            validationMessages = try report.errorMessages()
                + report.warningMessages()
            errorMessage = nil
        } catch let error as DecodingError {
            validation = nil
            validationMessages = ["The document is not valid JSON: \(error.localizedDescription)"]
        } catch {
            present(error, appModel: appModel)
        }
    }

    func save(client: WindexClient, appModel: AppModel) async {
        guard canSave else { return }
        isSaving = true
        defer { isSaving = false }
        do {
            let document = try decodeDocument()
            let saved: Recipe
            if isNew {
                saved = try await client.createRecipe(document)
            } else if let selectedName {
                saved = try await client.updateRecipe(
                    named: selectedName, document: document)
            } else {
                return
            }
            selectedName = saved.name
            isNew = false
            await load(client: client, appModel: appModel)
        } catch {
            present(error, appModel: appModel)
        }
    }

    private func decodeDocument() throws -> JSONValue {
        try JSONDecoder().decode(JSONValue.self, from: Data(documentText.utf8))
    }

    private func pretty(_ value: JSONValue) throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        return String(decoding: try encoder.encode(value), as: UTF8.self)
    }

    private func present(_ error: any Error, appModel: AppModel) {
        appModel.handleClientError(error)
        guard appModel.connectedBackend != nil else { return }
        errorMessage = (error as? WindexError)?.localizedDescription
            ?? "The recipe could not be loaded."
    }

    private static let template = """
        {
          "schema": "windex.recipe/1",
          "name": "my_source",
          "version": 1,
          "title": "My source",
          "description": "",
          "corpus": {
            "source": "my_source",
            "id_prefix": "my_source:",
            "collection": "my_source"
          },
          "config": [
            {
              "key": "seeds",
              "kind": "url_list",
              "required": true,
              "stage": "install",
              "label": "Seed URLs"
            }
          ],
          "state": {
            "frontier": {"key": "url", "order": "depth, seq"}
          },
          "flows": {
            "crawl": {
              "nodes": {
                "seed": {
                  "kind": "discover",
                  "uses": "crawl.frontier",
                  "with": {"store": "frontier", "seeds": "@config.seeds"}
                },
                "get": {"kind": "fetch", "uses": "http.get", "with": {}},
                "text": {
                  "kind": "extract",
                  "uses": "html.trafilatura",
                  "with": {}
                },
                "stage": {"kind": "load", "uses": "ledger.stage", "with": {}}
              },
              "edges": [["seed", "get"], ["get", "text"], ["text", "stage"]]
            }
          },
          "refresh": ["crawl"]
        }
        """
}

private enum RecipeEditorError: LocalizedError {
    case missingDocument

    var errorDescription: String? {
        "The server did not return the normalized recipe document."
    }
}

struct RecipesView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend

    @State private var model = RecipesModel()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        HSplitView {
            catalogue
                .frame(minWidth: 220, idealWidth: 260, maxWidth: 320)
            editor
                .frame(minWidth: 520)
            validation
                .frame(minWidth: 220, idealWidth: 280, maxWidth: 340)
        }
        .background(theme.palette.ink)
        .task(id: backend.profile) {
            await model.load(client: client, appModel: appModel)
        }
        .task(id: model.documentText) {
            do {
                try await Task.sleep(for: .milliseconds(450))
            } catch {
                return
            }
            await model.validate(client: client, appModel: appModel)
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack {
                StyledText("Recipes", Typography.masthead)
                Spacer()
                Button {
                    model.newRecipe()
                } label: {
                    Image(systemName: "plus")
                        .accessibilityLabel("New recipe")
                }
                .buttonStyle(.plain)
            }
            .padding(.md)
            Hairline()
            List(model.recipes, id: \.name, selection: selection) { recipe in
                VStack(alignment: .leading, spacing: .xxs) {
                    Text(recipe.displayTitle)
                        .windexStyle(Typography.label)
                    Text("\(recipe.name) · revision \(recipe.version ?? 1)")
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                }
                .tag(recipe.name)
            }
            .listStyle(.sidebar)
            .scrollContentBackground(.hidden)
        }
        .background(theme.palette.plate)
    }

    private var selection: Binding<String?> {
        Binding(
            get: { model.selectedName },
            set: { name in
                guard let name, name != model.selectedName else { return }
                Task {
                    await model.select(name, client: client, appModel: appModel)
                }
            })
    }

    private var editor: some View {
        VStack(spacing: 0) {
            HStack {
                VStack(alignment: .leading, spacing: .xxs) {
                    StyledText(
                        model.isNew ? "New recipe" : (model.selectedName ?? "Recipe"),
                        Typography.setLG)
                    Text("Normalized recipe document · JSON")
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                }
                Spacer()
                Button("Save") {
                    Task {
                        await model.save(client: client, appModel: appModel)
                    }
                }
                .buttonStyle(.borderedProminent)
                .disabled(!model.canSave)
            }
            .padding(.md)
            Hairline()
            TextEditor(text: $model.documentText)
                .font(.system(size: 12, design: .monospaced))
                .scrollContentBackground(.hidden)
                .padding(.sm)
                .textSelection(.enabled)
                .accessibilityLabel("Recipe document")
        }
        .background(theme.palette.ink)
    }

    private var validation: some View {
        VStack(alignment: .leading, spacing: .md) {
            StyledText("Validation", Typography.eyebrow)
            if let report = model.validation {
                StatusBadge(
                    report.valid ? .healthy : .fault,
                    word: report.valid ? "valid" : "invalid")
                Text("\(report.errors?.count ?? 0) errors · \(report.warnings?.count ?? 0) warnings")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            } else {
                StatusBadge(.attention, word: "checking")
            }
            Hairline()
            ForEach(Array(model.validationMessages.enumerated()), id: \.offset) {
                _, message in
                Text(message)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.amber)
            }
            if let error = model.errorMessage {
                Text(error)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.rust)
            }
            Spacer()
            Text("Recipe documents are inert. The server accepts only registered modules and validates every field before saving.")
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
        }
        .padding(.md)
        .background(theme.palette.plate)
    }
}
