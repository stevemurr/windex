import AppKit
import SwiftUI
import WindexKit
import WindexUI

struct LogsView: View {
    @Bindable var appModel: AppModel
    @Environment(BackendSession.self) private var session
    @Environment(\.windexTheme) private var theme
    @State private var presetName = ""

    var body: some View {
        @Bindable var store = session.logs

        VStack(spacing: 0) {
            controls(store: store)
            Hairline()
            HSplitView {
                eventTable(store: store)
                    .frame(minWidth: 620)
                inspector(store: store)
                    .frame(minWidth: 260, idealWidth: 320, maxWidth: 420)
            }
        }
        .background(theme.palette.ink)
        .task(id: appModel.consoleFilterRequest) {
            guard let filter = appModel.consoleFilterRequest else { return }
            store.filter = filter
            appModel.consoleFilterRequest = nil
            await session.loadLogHistory()
        }
        .task(id: store.filter) {
            try? await Task.sleep(for: .milliseconds(350))
            guard !Task.isCancelled else { return }
            await session.loadLogHistory()
        }
        .onChange(of: store.newestCursor) { _, newest in
            guard store.followsNewest, let newest else { return }
            store.selectedSequence = newest
        }
    }

    private func controls(store: SharedLogStore) -> some View {
        @Bindable var store = store

        return VStack(spacing: .sm) {
            HStack(spacing: .sm) {
                StyledText("Console", Typography.masthead)
                connectionStatus(store.connection)

                TextField(
                    "Filter event text",
                    text: Binding(
                        get: { store.filter.text },
                        set: { store.filter.text = $0 }))
                .textFieldStyle(.roundedBorder)
                .frame(minWidth: 220, maxWidth: 480)

                Spacer(minLength: 0)

                Toggle(
                    store.followsNewest ? "Follow" : "Paused",
                    isOn: $store.followsNewest)
                    .toggleStyle(.button)

                Button("Newest") {
                    store.selectedSequence = store.allEvents.last?.sequence
                    store.followsNewest = true
                }
                .disabled(store.allEvents.isEmpty)

                Menu {
                    Button("Copy visible Events") {
                        copy(store.events)
                    }
                    Button("Export visible Events…") {
                        export(store.events)
                    }
                    Divider()
                    Button("Clear local view") {
                        store.clearLocalView()
                    }
                } label: {
                    Image(systemName: "ellipsis")
                        .accessibilityLabel("Console actions")
                }
                .menuStyle(.borderlessButton)
                .fixedSize()
            }

            ScrollView(.horizontal) {
                HStack(spacing: .sm) {
                    Picker(
                        "Level",
                        selection: Binding(
                            get: { store.filter.levels.first },
                            set: { level in
                                store.filter.levels = level.map { [$0] } ?? []
                            })
                    ) {
                        Text("All levels").tag(OperationalEventLevel?.none)
                        ForEach(facetLevels(store), id: \.self) {
                            Text($0.rawValue).tag(Optional($0))
                        }
                    }
                    .frame(width: 120)

                    facetPicker(
                        "Component",
                        values: store.facets.components,
                        selection: Binding(
                            get: { store.filter.components.first },
                            set: { store.filter.components = $0.map { [$0] } ?? [] }
                        )
                    )
                    facetPicker(
                        "Source",
                        values: store.facets.sources,
                        selection: Binding(
                            get: { store.filter.sourceName },
                            set: { store.filter.sourceName = $0 }
                        )
                    )
                    facetPicker(
                        "Pipeline",
                        values: store.facets.pipelines,
                        selection: Binding(
                            get: { store.filter.pipelineName },
                            set: { store.filter.pipelineName = $0 }
                        )
                    )
                    TextField(
                        "Run",
                        text: Binding(
                            get: { store.filter.runID.map(String.init) ?? "" },
                            set: { store.filter.runID = Int($0) }
                        )
                    )
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 76)
                    facetPicker(
                        "Node",
                        values: store.facets.nodes,
                        selection: Binding(
                            get: { store.filter.node },
                            set: { store.filter.node = $0 }
                        )
                    )
                    facetPicker(
                        "Module",
                        values: store.facets.modules,
                        selection: Binding(
                            get: { store.filter.module },
                            set: { store.filter.module = $0 }
                        )
                    )

                    Toggle(
                        "Since",
                        isOn: Binding(
                            get: { store.filter.startedAt != nil },
                            set: { enabled in
                                store.filter.startedAt = enabled
                                    ? Date().addingTimeInterval(-3_600)
                                    : nil
                            }
                        )
                    )
                    if let startedAt = store.filter.startedAt {
                        DatePicker(
                            "Start",
                            selection: Binding(
                                get: { startedAt },
                                set: { store.filter.startedAt = $0 }
                            )
                        )
                        .labelsHidden()
                    }
                    Toggle(
                        "Until",
                        isOn: Binding(
                            get: { store.filter.endedAt != nil },
                            set: { enabled in
                                store.filter.endedAt = enabled ? Date() : nil
                            }
                        )
                    )
                    if let endedAt = store.filter.endedAt {
                        DatePicker(
                            "End",
                            selection: Binding(
                                get: { endedAt },
                                set: { store.filter.endedAt = $0 }
                            )
                        )
                        .labelsHidden()
                    }
                }
            }

            HStack(spacing: .sm) {
                Menu("Presets") {
                    if store.presets.isEmpty {
                        Text("No saved presets")
                    }
                    ForEach(store.presets) { preset in
                        Button(preset.name) {
                            store.applyPreset(preset.id)
                        }
                    }
                    if !store.presets.isEmpty {
                        Divider()
                        Menu("Delete preset") {
                            ForEach(store.presets) { preset in
                                Button(preset.name, role: .destructive) {
                                    store.deletePreset(preset.id)
                                }
                            }
                        }
                    }
                }
                TextField("Preset name", text: $presetName)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 160)
                Button("Save preset") {
                    store.savePreset(named: presetName)
                    presetName = ""
                }
                .disabled(presetName.trimmingCharacters(in: .whitespaces).isEmpty)
                Button("Load filtered history") {
                    Task { await session.loadLogHistory() }
                }
                Button("Next history page") {
                    Task { await session.loadLogHistory(continuing: true) }
                }
                .disabled(!store.historyHasMore)
                if case .loading = store.historyState {
                    ProgressView().controlSize(.small)
                } else if case .failed(let message) = store.historyState {
                    Text(message)
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.rust)
                        .lineLimit(1)
                }
                Spacer()
                Button("Clear filters") {
                    store.filter = OperationalEventFilter()
                }
                .disabled(store.filter.isEmpty)
            }
        }
        .padding(.md)
        .background(theme.palette.plate)
    }

    private func facetPicker(
        _ title: String,
        values: [String],
        selection: Binding<String?>
    ) -> some View {
        Picker(title, selection: selection) {
            Text("All \(title.lowercased())s").tag(String?.none)
            ForEach(values, id: \.self) { Text($0).tag(Optional($0)) }
        }
        .frame(width: 140)
    }

    private func facetLevels(_ store: SharedLogStore) -> [OperationalEventLevel] {
        let levels = store.facets.levels.compactMap { raw -> OperationalEventLevel? in
            raw == "warn" ? .warning : .init(rawValue: raw)
        }
        return levels.isEmpty ? OperationalEventLevel.allCases : levels
    }

    private func connectionStatus(_ value: LiveConnectionState) -> some View {
        Group {
            switch value {
            case .live:
                StatusBadge(.running, word: "live")
            case .degraded:
                StatusBadge(.attention, word: "degraded")
            case .connecting:
                StatusBadge(.running, word: "connecting")
            case .idle:
                StatusBadge(.attention, word: "awaiting stream")
            }
        }
    }

    private func eventTable(store: SharedLogStore) -> some View {
        @Bindable var store = store

        return Group {
            if store.events.isEmpty {
                VStack(alignment: .leading, spacing: .sm) {
                    Text(
                        store.allEvents.isEmpty
                            ? "No Events collected yet."
                            : "No Events match this filter."
                    )
                    .windexStyle(Typography.label)
                    Text(
                        store.allEvents.isEmpty
                            ? "Console history and the follow stream will appear here without manual refresh."
                            : "Change the filters to return to the buffered window."
                    )
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                .padding(.xl)
            } else {
                Table(store.events, selection: $store.selectedSequence) {
                    TableColumn("Time") { event in
                        Text(event.timestamp, format: .dateTime.hour().minute().second())
                            .windexStyle(Typography.dataSM)
                    }
                    .width(min: 76, ideal: 86, max: 100)

                    TableColumn("Level") { event in
                        Text(event.level.rawValue)
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(levelColor(event.level))
                    }
                    .width(min: 62, ideal: 72, max: 90)

                    TableColumn("Source") { event in
                        Text(event.sourceName ?? "—")
                            .windexStyle(Typography.dataSM)
                    }
                    .width(min: 80, ideal: 110, max: 160)

                    TableColumn("Pipeline / Run") { event in
                        Text(pipelineRun(event))
                            .windexStyle(Typography.dataSM)
                    }
                    .width(min: 120, ideal: 170, max: 240)

                    TableColumn("Component") { event in
                        Text(event.component)
                            .windexStyle(Typography.dataSM)
                    }
                    .width(min: 90, ideal: 120, max: 180)

                    TableColumn("Message") { event in
                        Text(event.message)
                            .windexStyle(Typography.dataSM)
                            .lineLimit(1)
                    }
                    .width(min: 220, ideal: 420)
                }
                .tableStyle(.inset(alternatesRowBackgrounds: false))
            }
        }
    }

    @ViewBuilder
    private func inspector(store: SharedLogStore) -> some View {
        if let event = store.selectedEvent {
            ScrollView {
                VStack(alignment: .leading, spacing: .md) {
                    StyledText(event.event, Typography.masthead)
                    Text(event.message)
                        .windexStyle(Typography.body)
                        .textSelection(.enabled)
                    Hairline()
                    metadata("Sequence", String(event.sequence))
                    metadata(
                        "Time",
                        event.timestamp.formatted(
                            .iso8601.year().month().day().dateSeparator(.dash)
                                .time(includingFractionalSeconds: true)))
                    metadata("Level", event.level.rawValue)
                    metadata("Component", event.component)
                    if let source = event.sourceName { metadata("Source", source) }
                    if let pipeline = event.pipelineName {
                        metadata(
                            "Pipeline",
                            pipeline + (event.pipelineVersion.map { " @ \($0)" } ?? ""))
                    }
                    if let runID = event.runID { metadata("Run", String(runID)) }
                    if let node = event.node { metadata("Node", node) }
                    if let module = event.module { metadata("Module", module) }
                    if !event.data.isEmpty {
                        Hairline()
                        StyledText("Data", Typography.eyebrow)
                            .foregroundStyle(theme.palette.graphite)
                        ForEach(event.data.keys.sorted(), id: \.self) { key in
                            metadata(key, event.data[key]?.displayString ?? "")
                        }
                    }
                }
                .padding(.md)
            }
            .background(theme.palette.plate)
        } else {
            Text("Select an Event to inspect structured metadata.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .padding(.md)
                .background(theme.palette.plate)
        }
    }

    private func metadata(_ key: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: .xxs) {
            Text(key)
                .windexStyle(Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            Text(value)
                .windexStyle(Typography.dataSM)
                .textSelection(.enabled)
        }
    }

    private func pipelineRun(_ event: OperationalEvent) -> String {
        let pipeline = event.pipelineName.map {
            $0 + (event.pipelineVersion.map { " @ \($0)" } ?? "")
        }
        let run = event.runID.map { "run \($0)" }
        return [pipeline, run].compactMap { $0 }.joined(separator: " · ")
    }

    private func levelColor(_ level: OperationalEventLevel) -> Color {
        switch level {
        case .trace, .debug, .info:
            theme.palette.graphite
        case .warning:
            theme.palette.amber
        case .error, .critical:
            theme.palette.rust
        }
    }

    private func copy(_ events: [OperationalEvent]) {
        let lines = events.map {
            "\($0.timestamp.formatted(.iso8601))\t\($0.level.rawValue)\t\($0.component)\t\($0.message)"
        }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(lines.joined(separator: "\n"), forType: .string)
    }

    private func export(_ events: [OperationalEvent]) {
        let panel = NSSavePanel()
        panel.nameFieldStringValue = "windex-events.json"
        panel.allowedContentTypes = [.json]
        guard panel.runModal() == .OK, let url = panel.url else { return }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        guard let data = try? encoder.encode(events) else { return }
        try? data.write(to: url, options: .atomic)
    }
}
