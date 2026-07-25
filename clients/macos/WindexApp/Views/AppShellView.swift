import SwiftUI
import WindexKit
import WindexUI

struct AppShellView: View {
    @Bindable var model: AppModel
    @Environment(\.windexTheme) private var theme

    var body: some View {
        Group {
            if let backend = model.connectedBackend,
               let client = model.client {
                connectedShell(client: client, backend: backend)
            } else {
                PairingView(model: model)
            }
        }
        .frame(
            minWidth: Layout.minimumWindow.width,
            minHeight: Layout.minimumWindow.height)
        .background(theme.palette.ink)
        .foregroundStyle(theme.palette.paper)
        .tint(theme.palette.cyan)
    }

    private func connectedShell(
        client: WindexClient,
        backend: ConnectedBackend
    ) -> some View {
        NavigationSplitView {
            sidebar(backend: backend)
        } detail: {
            destination(client: client, backend: backend)
        }
        .navigationSplitViewStyle(.balanced)
    }

    private func sidebar(backend: ConnectedBackend) -> some View {
        List(SidebarDestination.allCases, selection: $model.selection) { destination in
            Label(destination.title, systemImage: destination.systemImage)
                .tag(destination)
                .windexStyle(Typography.label)
                .accessibilityLabel(destination.title)
        }
        .listStyle(.sidebar)
        .navigationTitle("Windex")
        .safeAreaInset(edge: .bottom) {
            ConnectionFooter(backend: backend) {
                model.forgetBackend()
            }
        }
    }

    @ViewBuilder
    private func destination(
        client: WindexClient,
        backend: ConnectedBackend
    ) -> some View {
        switch model.selection {
        case .overview:
            OverviewView(appModel: model, client: client, backend: backend)
        case .sources:
            SourcesView(appModel: model, client: client, backend: backend)
        case .settings:
            SettingsView(appModel: model, client: client, backend: backend)
        case .logs:
            LogsView(appModel: model, client: client, backend: backend)
        case .search:
            SearchView(appModel: model, client: client, backend: backend)
        case .runs:
            CapabilityGateView(
                title: "Runs",
                summary: "Run history and the live Galley need the generic runs API.",
                detail: "The backend can compile recipe tasks today, but it cannot create or list recipe runs yet. Showing legacy job activity here would imply those are the same execution model.",
                prerequisite: "Waiting for recipe run creation, history, and event streaming.")
        case .recipes:
            CapabilityGateView(
                title: "Recipe editor",
                summary: "The module palette and validator are ready; recipe writes are not.",
                detail: "Source definitions can be opened in Sources now. Editing stays disabled until the backend can persist a validated recipe without inventing a client-only format.",
                prerequisite: "Waiting for recipe create and update endpoints.",
                actionTitle: "Open Sources") {
                    model.selection = .sources
                }
        }
    }
}

private struct ConnectionFooter: View {
    let backend: ConnectedBackend
    let forget: () -> Void
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .xs) {
            Hairline()
            HStack(spacing: .xs) {
                StatusBadge(
                    .healthy,
                    word: backend.hasStoredToken
                        ? "paired" : "open backend")
                Spacer(minLength: 0)
                Menu {
                    Button("Change backend", action: forget)
                } label: {
                    Image(systemName: "ellipsis")
                        .accessibilityLabel("Connection options")
                }
                .menuStyle(.borderlessButton)
                .fixedSize()
            }
            Text(backend.profile.displayAddress)
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
                .lineLimit(1)
                .truncationMode(.middle)
        }
        .padding(.horizontal, .md)
        .padding(.vertical, .sm)
        .background(theme.palette.plate)
    }
}

private struct CapabilityGateView: View {
    let title: String
    let summary: String
    let detail: String
    let prerequisite: String
    var actionTitle: String?
    var action: (() -> Void)?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .lg) {
            StyledText(title, Typography.setLG)
            Hairline()
            StatusBadge(.attention, word: "backend capability unavailable")
            Text(summary)
                .windexStyle(Typography.label)
                .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
            Text(detail)
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
            Text(prerequisite)
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.amber)
                .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
            if let actionTitle, let action {
                Button(actionTitle, action: action)
                    .buttonStyle(.bordered)
            }
            Spacer()
        }
        .padding(.xl)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(theme.palette.ink)
    }
}
