import Observation
import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class MarketplaceModel {
    private(set) var entries: [MarketplaceEntry] = []
    private(set) var form: FormModel?
    private(set) var isLoading = false
    private(set) var isActing = false
    private(set) var errorMessage: String?
    private(set) var confirmation: String?
    var selectedID: String?
    var installName = ""

    var selected: MarketplaceEntry? {
        guard let selectedID else { return nil }
        return entries.first { $0.id == selectedID }
    }

    var canInstall: Bool {
        guard let selected, selected.installed != true, let form else {
            return false
        }
        return !installName.isEmpty && form.errors.isEmpty && !isActing
    }

    func load(client: WindexClient, appModel: AppModel) async {
        isLoading = entries.isEmpty
        do {
            entries = try await client.marketplace()
            isLoading = false
            errorMessage = nil
            let id = selectedID.flatMap { selected in
                entries.first(where: { $0.id == selected })?.id
            } ?? entries.first?.id
            try select(id)
        } catch {
            isLoading = false
            present(error, appModel: appModel)
        }
    }

    func select(_ id: String?) throws {
        selectedID = id
        confirmation = nil
        errorMessage = nil
        guard let entry = selected else {
            form = nil
            installName = ""
            return
        }
        form = FormModel(params: try entry.installParameters())
        installName = entry.installedName ?? entry.name
    }

    func install(client: WindexClient, appModel: AppModel) async {
        guard canInstall, let selected, let form else { return }
        isActing = true
        defer { isActing = false }
        do {
            let recipe = try await client.installMarketplaceEntry(
                id: selected.id,
                name: installName,
                values: form.values)
            await load(client: client, appModel: appModel)
            confirmation = "\(recipe.displayTitle) installed."
        } catch {
            present(error, appModel: appModel)
        }
    }

    func update(client: WindexClient, appModel: AppModel) async {
        guard let selected, selected.updateAvailable == true,
              selected.locallyEdited != true else { return }
        isActing = true
        defer { isActing = false }
        do {
            let recipe = try await client.updateMarketplaceEntry(id: selected.id)
            await load(client: client, appModel: appModel)
            confirmation = "\(recipe.displayTitle) updated."
        } catch {
            present(error, appModel: appModel)
        }
    }

    private func present(_ error: any Error, appModel: AppModel) {
        appModel.handleClientError(error)
        guard appModel.connectedBackend != nil else { return }
        errorMessage = (error as? WindexError)?.localizedDescription
            ?? "The marketplace could not be loaded."
    }
}

struct MarketplaceView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend

    @State private var model = MarketplaceModel()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        GeometryReader { geometry in
            if geometry.size.width >= 800 {
                HSplitView {
                    catalogue
                        .frame(minWidth: 220, idealWidth: 270, maxWidth: 340)
                    detail
                        .frame(minWidth: 520)
                }
            } else if model.selectedID == nil {
                catalogue
            } else {
                detail
            }
        }
        .background(theme.palette.ink)
        .task(id: backend.profile) {
            await model.load(client: client, appModel: appModel)
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack {
                StyledText("Marketplace", Typography.masthead)
                Spacer()
                Button {
                    Task { await model.load(client: client, appModel: appModel) }
                } label: {
                    Image(systemName: "arrow.clockwise")
                        .accessibilityLabel("Refresh marketplace")
                }
                .buttonStyle(.plain)
                .disabled(model.isLoading)
            }
            .padding(.md)
            Hairline()
            if model.isLoading, model.entries.isEmpty {
                ProgressView()
                    .controlSize(.small)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if model.entries.isEmpty {
                Text("No catalog recipes are configured.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
                    .padding(.lg)
                    .frame(maxWidth: .infinity, maxHeight: .infinity,
                           alignment: .topLeading)
            } else {
                List(model.entries, id: \.id, selection: selection) { entry in
                    VStack(alignment: .leading, spacing: .xxs) {
                        HStack {
                            Text(entry.title)
                                .windexStyle(Typography.label)
                            Spacer(minLength: 0)
                            if entry.updateAvailable == true {
                                StatusBadge(.attention, word: "update")
                            } else if entry.installed == true {
                                StatusBadge(.healthy, word: "installed")
                            }
                        }
                        Text("\(entry.catalog) · version \(entry.version)")
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.graphite)
                    }
                    .tag(entry.id)
                }
                .listStyle(.sidebar)
                .scrollContentBackground(.hidden)
            }
        }
        .background(theme.palette.plate)
    }

    private var selection: Binding<String?> {
        Binding(
            get: { model.selectedID },
            set: { id in
                do {
                    try model.select(id)
                } catch {
                    appModel.handleClientError(error)
                }
            })
    }

    @ViewBuilder
    private var detail: some View {
        if let entry = model.selected {
            ScrollView {
                VStack(alignment: .leading, spacing: .lg) {
                    HStack(alignment: .firstTextBaseline) {
                        StyledText(entry.title, Typography.setLG)
                        Spacer()
                        if entry.installed == true {
                            StatusBadge(.healthy, word: "installed")
                        }
                    }
                    if let description = entry.description, !description.isEmpty {
                        Text(description)
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.graphite)
                            .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
                    }
                    Text("Catalog \(entry.catalog) · recipe \(entry.name) · version \(entry.version)")
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                    Hairline()

                    availability(entry)

                    if entry.installed != true, let form = model.form {
                        installForm(entry, form: form)
                    } else {
                        installedActions(entry)
                    }

                    if let error = model.errorMessage {
                        Text(error)
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.rust)
                    } else if let confirmation = model.confirmation {
                        Text(confirmation)
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.graphite)
                    }
                    Hairline()
                    Text("Catalog recipes are inert YAML. Installation can reference only modules shipped by this server; it never downloads or executes catalog code.")
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                        .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
                }
                .padding(.xl)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        } else if let error = model.errorMessage {
            SourceFailureView(message: error) {
                Task { await model.load(client: client, appModel: appModel) }
            }
        } else {
            Text("Choose a catalog recipe.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }

    private func availability(_ entry: MarketplaceEntry) -> some View {
        VStack(alignment: .leading, spacing: .xs) {
            StyledText("Availability", Typography.eyebrow)
            if entry.executable == true {
                StatusBadge(.healthy, word: "ready to run")
            } else {
                StatusBadge(.attention, word: "installable · execution pending")
                Text((entry.unavailableModules ?? []).joined(separator: ", "))
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
                    .textSelection(.enabled)
            }
        }
    }

    private func installForm(
        _ entry: MarketplaceEntry,
        form: FormModel
    ) -> some View {
        VStack(alignment: .leading, spacing: .md) {
            Hairline()
            StyledText("Install", Typography.eyebrow)
            TextField("Recipe name", text: $model.installName)
                .textFieldStyle(.roundedBorder)
                .frame(maxWidth: 360)
                .accessibilityLabel("Installed recipe name")
            SchemaForm(model: form)
            Button("Install recipe") {
                Task {
                    await model.install(client: client, appModel: appModel)
                }
            }
            .buttonStyle(.borderedProminent)
            .disabled(!model.canInstall)
        }
    }

    private func installedActions(_ entry: MarketplaceEntry) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            Hairline()
            if entry.locallyEdited == true, entry.updateAvailable == true {
                StatusBadge(.attention, word: "local edits preserved")
                Text("Upstream changed, but update is disabled because it would overwrite local edits.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            } else if entry.updateAvailable == true {
                Button("Update recipe") {
                    Task {
                        await model.update(client: client, appModel: appModel)
                    }
                }
                .buttonStyle(.borderedProminent)
                .disabled(model.isActing)
            } else {
                Text("\(entry.installedName ?? entry.name) is current.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }
        }
    }
}
