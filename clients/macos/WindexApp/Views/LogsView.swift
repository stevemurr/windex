import Observation
import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class LogsModel {
    private(set) var sources: [LogSource] = []
    private(set) var tail: LogTail?
    private(set) var isLoading = false
    private(set) var errorMessage: String?
    var selectedName: String?
    var grep = ""
    var level: WindexClient.LogLevel?

    func run(client: WindexClient, appModel: AppModel) async {
        guard await loadCatalogue(client: client, appModel: appModel) else { return }
        while !Task.isCancelled {
            do {
                try await Task.sleep(for: .seconds(5))
            } catch {
                return
            }
            await refreshTail(client: client, appModel: appModel, quietly: true)
        }
    }

    @discardableResult
    func loadCatalogue(client: WindexClient, appModel: AppModel) async -> Bool {
        isLoading = sources.isEmpty
        do {
            sources = try await client.logs().sorted {
                let left = $0.category ?? ""
                let right = $1.category ?? ""
                if left != right { return left < right }
                return ($0.title ?? $0.name)
                    .localizedStandardCompare($1.title ?? $1.name) == .orderedAscending
            }
            if selectedName == nil {
                selectedName = sources.first(where: { $0.available == true })?.name
                    ?? sources.first?.name
            }
            isLoading = false
            errorMessage = nil
            await refreshTail(client: client, appModel: appModel)
            return true
        } catch {
            isLoading = false
            present(error, appModel: appModel)
            return false
        }
    }

    func select(_ name: String?, client: WindexClient, appModel: AppModel) async {
        selectedName = name
        tail = nil
        await refreshTail(client: client, appModel: appModel)
    }

    func refreshTail(
        client: WindexClient,
        appModel: AppModel,
        quietly: Bool = false
    ) async {
        guard let selectedName else { return }
        if !quietly { isLoading = tail == nil }
        do {
            let response = try await client.logTail(
                name: selectedName,
                lines: 500,
                grep: grep.isEmpty ? nil : grep,
                level: level)
            guard self.selectedName == selectedName else { return }
            tail = response
            isLoading = false
            errorMessage = nil
        } catch {
            guard self.selectedName == selectedName else { return }
            isLoading = false
            present(error, appModel: appModel)
        }
    }

    private func present(_ error: any Error, appModel: AppModel) {
        appModel.handleClientError(error)
        guard appModel.connectedBackend != nil else { return }
        errorMessage = (error as? WindexError)?.localizedDescription
            ?? "Logs could not be loaded."
    }
}

struct LogsView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend

    @State private var model = LogsModel()
    @Environment(\.windexTheme) private var theme

    var body: some View {
        GeometryReader { geometry in
            if geometry.size.width >= 800 {
                HSplitView {
                    catalogue
                        .frame(minWidth: 220, idealWidth: 270, maxWidth: 340)
                    logReader
                        .frame(minWidth: 520)
                }
            } else if model.selectedName == nil {
                catalogue
            } else {
                logReader
                    .toolbar {
                        ToolbarItem(placement: .navigation) {
                            Button {
                                Task {
                                    await model.select(
                                        nil,
                                        client: client,
                                        appModel: appModel)
                                }
                            } label: {
                                Label("All logs", systemImage: "chevron.left")
                            }
                        }
                    }
            }
        }
        .background(theme.palette.ink)
        .task(id: backend.profile) {
            await model.run(client: client, appModel: appModel)
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack {
                StyledText("Logs", Typography.masthead)
                Spacer()
                Button {
                    Task {
                        await model.loadCatalogue(client: client, appModel: appModel)
                    }
                } label: {
                    Image(systemName: "arrow.clockwise")
                        .accessibilityLabel("Refresh log catalogue")
                }
                .buttonStyle(.plain)
            }
            .padding(.md)
            Hairline()

            List(model.sources, id: \.name, selection: selectedName) { source in
                VStack(alignment: .leading, spacing: .xxs) {
                    HStack {
                        Text(source.title ?? source.name)
                            .windexStyle(Typography.label)
                        Spacer()
                        StatusBadge(
                            source.available == true ? .healthy : .attention,
                            word: source.available == true ? nil : "not written")
                    }
                    HStack(spacing: .xs) {
                        Text(source.category ?? source.kind ?? "log")
                        if let size = source.size {
                            Text("· \(ByteCountFormatter.string(fromByteCount: Int64(size), countStyle: .file))")
                        }
                    }
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
                }
                .tag(source.name)
                .accessibilityElement(children: .combine)
            }
            .listStyle(.sidebar)
            .scrollContentBackground(.hidden)
        }
        .background(theme.palette.plate)
    }

    private var selectedName: Binding<String?> {
        Binding(
            get: { model.selectedName },
            set: { name in
                guard name != model.selectedName else { return }
                Task {
                    await model.select(name, client: client, appModel: appModel)
                }
            })
    }

    private var logReader: some View {
        VStack(spacing: 0) {
            controls
            Hairline()
            Group {
                if model.isLoading, model.tail == nil {
                    ProgressView()
                        .controlSize(.small)
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else if let error = model.errorMessage, model.tail == nil {
                    SourceFailureView(message: error) {
                        Task {
                            await model.refreshTail(
                                client: client,
                                appModel: appModel)
                        }
                    }
                } else if let tail = model.tail, tail.available == false {
                    Text("This log has not been written yet.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else {
                    lines
                }
            }
        }
        .background(theme.palette.ink)
    }

    private var controls: some View {
        HStack(spacing: .sm) {
            VStack(alignment: .leading, spacing: .xxs) {
                StyledText(
                    model.sources.first(where: { $0.name == model.selectedName })?
                        .title ?? model.selectedName ?? "Choose a log",
                    Typography.masthead)
                if model.tail?.truncated == true {
                    Text("showing the newest 500 lines · older output truncated")
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.amber)
                }
            }
            Spacer()
            TextField("Filter text", text: $model.grep)
                .textFieldStyle(.roundedBorder)
                .frame(width: 180)
                .onSubmit {
                    Task {
                        await model.refreshTail(client: client, appModel: appModel)
                    }
                }
                .accessibilityLabel("Filter log text")
            Picker("Level", selection: $model.level) {
                Text("All levels").tag(WindexClient.LogLevel?.none)
                ForEach(WindexClient.LogLevel.allCases, id: \.self) { level in
                    Text(level.rawValue).tag(Optional(level))
                }
            }
            .frame(width: 120)
            .onChange(of: model.level) {
                Task {
                    await model.refreshTail(client: client, appModel: appModel)
                }
            }
            Button {
                Task {
                    await model.refreshTail(client: client, appModel: appModel)
                }
            } label: {
                Image(systemName: "arrow.clockwise")
                    .accessibilityLabel("Refresh log")
            }
            .buttonStyle(.plain)
        }
        .padding(.md)
    }

    private var lines: some View {
        ScrollView([.horizontal, .vertical]) {
            Text((model.tail?.lines ?? []).joined(separator: "\n"))
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.paper)
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .topLeading)
                .padding(.md)
        }
    }
}
