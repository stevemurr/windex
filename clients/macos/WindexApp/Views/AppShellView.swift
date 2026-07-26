import SwiftUI
import WindexKit
import WindexUI

struct AppShellView: View {
    @Bindable var model: AppModel
    @Environment(\.windexTheme) private var theme
    @Environment(\.scenePhase) private var scenePhase

    var body: some View {
        Group {
            if let backend = model.connectedBackend,
               let client = model.client,
               let session = model.session {
                connectedShell(client: client, backend: backend, session: session)
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
        backend: ConnectedBackend,
        session: BackendSession
    ) -> some View {
        NavigationSplitView {
            sidebar(backend: backend)
        } detail: {
            destination(client: client, backend: backend)
        }
        .navigationSplitViewStyle(.balanced)
        .environment(session)
        .task(id: backend.profile) {
            await session.start()
        }
        .onChange(of: scenePhase) { _, phase in
            guard phase == .active else { return }
            Task { await session.foreground() }
        }
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
            SourcesView(appModel: model)
        case .pipelines:
            PipelinesView(appModel: model)
        case .settings:
            SettingsView(appModel: model, client: client, backend: backend)
        case .logs:
            LogsView(appModel: model)
        case .search:
            SearchView(appModel: model, client: client, backend: backend)
        case .runs:
            RunsView(appModel: model)
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
