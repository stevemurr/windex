import Observation
import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class SettingsModel {
    private(set) var scopes: [SettingsScope] = []
    private(set) var form: FormModel?
    private(set) var isLoading = false
    private(set) var isSaving = false
    private(set) var errorMessage: String?
    private(set) var confirmation: String?
    var selectedScope: String?
    private var etags: [String: String] = [:]

    func load(client: WindexClient, session: BackendSession, appModel: AppModel) async {
        isLoading = scopes.isEmpty
        do {
            let global = try await client.globalSettings()
            var loaded = [try global.settingsScope()]
            etags[global.scope] = global.etag
            for source in session.sources.sources {
                let response = try await client.sourceSettings(source.name)
                loaded.append(try response.settingsScope())
                etags[source.name] = response.etag
            }
            scopes = loaded.sorted {
                if $0.isGlobal != $1.isGlobal { return $0.isGlobal }
                return $0.scope.localizedStandardCompare($1.scope) == .orderedAscending
            }
            let scopeName = selectedScope.flatMap { selected in
                scopes.first(where: { $0.scope == selected })?.scope
            } ?? scopes.first?.scope
            select(scopeName)
            isLoading = false
            errorMessage = nil
        } catch {
            isLoading = false
            present(error, appModel: appModel)
        }
    }

    func select(_ scope: String?) {
        selectedScope = scope
        guard let scope,
              let match = scopes.first(where: { $0.scope == scope }) else {
            form = nil
            return
        }
        form = FormModel(scope: match)
        confirmation = nil
        errorMessage = nil
    }

    func save(client: WindexClient, appModel: AppModel) async {
        guard let selectedScope, let form, form.canSubmit else { return }
        isSaving = true
        confirmation = nil
        do {
            guard let etag = etags[selectedScope] else {
                throw WindexError.preconditionRequired(message: "Settings ETag is unavailable.")
            }
            let updated: SettingsScope
            if selectedScope == SettingsScope.global {
                let response = try await client.patchGlobalSettings(form.changes, etag: etag)
                etags[selectedScope] = response.etag
                updated = try response.settingsScope()
            } else {
                let response = try await client.patchSourceSettings(
                    selectedScope, values: form.changes, etag: etag)
                etags[selectedScope] = response.etag
                updated = try response.settingsScope()
            }
            replace(updated); form.apply(updated.fields)
            confirmation = "Settings saved."
            isSaving = false
            errorMessage = nil
        } catch {
            isSaving = false
            errorMessage = form.apply(error)
            appModel.handleClientError(error)
        }
    }

    func revert(
        key: String,
        client: WindexClient,
        appModel: AppModel
    ) async {
        guard let selectedScope else { return }
        isSaving = true
        confirmation = nil
        do {
            guard let etag = etags[selectedScope] else {
                throw WindexError.preconditionRequired(message: "Settings ETag is unavailable.")
            }
            let updated: SettingsScope
            if selectedScope == SettingsScope.global {
                let response = try await client.deleteGlobalSetting(key, etag: etag)
                etags[selectedScope] = response.etag
                updated = try response.settingsScope()
            } else {
                let response = try await client.deleteSourceSetting(
                    selectedScope, key: key, etag: etag)
                etags[selectedScope] = response.etag
                updated = try response.settingsScope()
            }
            replace(updated)
            form = FormModel(scope: updated)
            confirmation = "\(key) now follows its environment or default value."
            isSaving = false
            errorMessage = nil
        } catch {
            isSaving = false
            present(error, appModel: appModel)
        }
    }

    var selectedSettingsScope: SettingsScope? {
        guard let selectedScope else { return nil }
        return scopes.first { $0.scope == selectedScope }
    }

    private func replace(_ scope: SettingsScope) {
        if let index = scopes.firstIndex(where: { $0.scope == scope.scope }) {
            scopes[index] = scope
        }
    }

    private func present(_ error: any Error, appModel: AppModel) {
        appModel.handleClientError(error)
        guard appModel.connectedBackend != nil else { return }
        errorMessage = (error as? WindexError)?.localizedDescription
            ?? "Settings could not be loaded."
    }
}

struct SettingsView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend
    @Environment(BackendSession.self) private var session

    @State private var model = SettingsModel()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        GeometryReader { geometry in
            if geometry.size.width >= 800 {
                HSplitView {
                    scopeList
                        .frame(minWidth: 190, idealWidth: 220, maxWidth: 280)
                    editor
                        .frame(minWidth: 520)
                }
            } else if model.selectedScope == nil {
                scopeList
            } else {
                editor
                    .toolbar {
                        ToolbarItem(placement: .navigation) {
                            Button {
                                model.select(nil)
                            } label: {
                                Label("All settings", systemImage: "chevron.left")
                            }
                        }
                    }
            }
        }
        .background(theme.palette.ink)
        .task(id: backend.profile) {
            await model.load(client: client, session: session, appModel: appModel)
        }
    }

    private var scopeList: some View {
        VStack(spacing: 0) {
            HStack {
                StyledText("Settings", Typography.masthead)
                Spacer()
                Button {
                    Task {
                        await model.load(client: client, session: session, appModel: appModel)
                    }
                } label: {
                    Image(systemName: "arrow.clockwise")
                        .accessibilityLabel("Refresh settings")
                }
                .buttonStyle(.plain)
            }
            .padding(.md)
            Hairline()

            if model.isLoading && model.scopes.isEmpty {
                ProgressView()
                    .controlSize(.small)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                List(model.scopes, selection: selectedScope) { scope in
                    VStack(alignment: .leading, spacing: .xxs) {
                        Text(scope.isGlobal ? "Global" : scope.scope)
                            .windexStyle(Typography.label)
                        Text("\(scope.fields.count) settings")
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.graphite)
                    }
                    .tag(scope.scope)
                }
                .listStyle(.sidebar)
                .scrollContentBackground(.hidden)
            }
        }
        .background(theme.palette.plate)
    }

    private var selectedScope: Binding<String?> {
        Binding(
            get: { model.selectedScope },
            set: { model.select($0) })
    }

    @ViewBuilder
    private var editor: some View {
        if let form = model.form, let scope = model.selectedSettingsScope {
            ScrollView {
                VStack(alignment: .leading, spacing: .lg) {
                    HStack(alignment: .firstTextBaseline) {
                        StyledText(
                            scope.isGlobal ? "Global settings" : scope.scope,
                            Typography.setLG)
                        Spacer()
                        if model.isSaving {
                            ProgressView()
                                .controlSize(.small)
                                .accessibilityLabel("Saving settings")
                        }
                    }
                    Text("Values are rendered from the backend schema. Runtime overrides take precedence over environment and defaults.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                        .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
                    Hairline()

                    SchemaForm(model: form)

                    HStack(spacing: .sm) {
                        Button("Save changes") {
                            Task {
                                await model.save(client: client, appModel: appModel)
                            }
                        }
                        .buttonStyle(.borderedProminent)
                        .disabled(!form.canSubmit || model.isSaving)

                        Button("Revert edits") {
                            form.reset()
                        }
                        .buttonStyle(.bordered)
                        .disabled(!form.isDirty || model.isSaving)
                    }

                    if let error = model.errorMessage {
                        notice(error, colour: theme.palette.rust)
                    } else if let confirmation = model.confirmation {
                        notice(confirmation, colour: theme.palette.graphite)
                    }

                    overrides(scope)
                }
                .padding(.xl)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        } else if let error = model.errorMessage {
            SourceFailureView(message: error) {
                Task { await model.load(client: client, session: session, appModel: appModel) }
            }
        } else {
            ProgressView()
                .controlSize(.small)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }

    private func overrides(_ scope: SettingsScope) -> some View {
        let fields = scope.fields.filter { $0.origin?.isOverride == true }
        return VStack(alignment: .leading, spacing: .xs) {
            Hairline()
            StyledText("Runtime overrides", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            if fields.isEmpty {
                Text("No database overrides in this scope.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            } else {
                ForEach(fields) { field in
                    HStack {
                        Text(field.param.title)
                            .windexStyle(Typography.label)
                        Spacer()
                        Text(field.value?.displayString ?? "set")
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.graphite)
                            .lineLimit(1)
                        Button("Use default") {
                            Task {
                                await model.revert(
                                    key: field.key,
                                    client: client,
                                    appModel: appModel)
                            }
                        }
                        .buttonStyle(.borderless)
                        .disabled(model.isSaving)
                    }
                    .frame(maxWidth: Layout.proseMeasure)
                }
            }
        }
    }

    private func notice(_ message: String, colour: Color) -> some View {
        Text(message)
            .windexStyle(Typography.body)
            .foregroundStyle(colour)
            .textSelection(.enabled)
            .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
    }
}
