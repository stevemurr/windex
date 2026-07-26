import SwiftUI
import WindexKit
import WindexUI

/// The canonical Source surface. A Source is a deployment of an immutable
/// Pipeline revision, not an alias for a Pipeline.
struct SourcesView: View {
    @Bindable var appModel: AppModel
    @Environment(BackendSession.self) private var session
    @State private var selectedName: String?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        HSplitView {
            catalogue
                .frame(minWidth: 240, idealWidth: 280, maxWidth: 340)
            detail
                .frame(minWidth: 560)
        }
        .background(theme.palette.ink)
        .onChange(of: session.sources.sources) { _, sources in
            if selectedName == nil {
                selectedName = sources.first?.name
            } else if !sources.contains(where: { $0.name == selectedName }) {
                selectedName = sources.first?.name
            }
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack(alignment: .firstTextBaseline) {
                StyledText("Sources", Typography.masthead)
                Spacer()
                Button {
                    appModel.selection = .pipelines
                } label: {
                    Label("New source", systemImage: "plus")
                }
                .help("Choose or build a Pipeline before creating a Source.")
            }
            .padding(.md)

            Hairline()

            if session.sources.sources.isEmpty {
                emptyCatalogue
            } else {
                List(session.sources.sources, selection: $selectedName) { source in
                    SourceDeploymentRow(source: source)
                        .tag(source.name)
                }
                .listStyle(.sidebar)
                .scrollContentBackground(.hidden)
            }
        }
        .background(theme.palette.plate)
    }

    private var emptyCatalogue: some View {
        VStack(alignment: .leading, spacing: .md) {
            Text("No sources yet.")
                .windexStyle(Typography.label)
            Text(
                "A Source binds an immutable Pipeline revision to an external origin, configuration, runtime state, and searchable corpus."
            )
            .windexStyle(Typography.body)
            .foregroundStyle(theme.palette.graphite)
            .fixedSize(horizontal: false, vertical: true)

            Button("Choose a Pipeline") {
                appModel.selection = .pipelines
            }

            if case .idle = session.sources.state {
                Text("Waiting for the canonical Sources API.")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
            Spacer()
        }
        .padding(.lg)
    }

    @ViewBuilder
    private var detail: some View {
        if let selectedName,
           let source = session.sources.sources.first(where: { $0.name == selectedName }) {
            SourceDeploymentDetail(source: source) {
                appModel.selection = .pipelines
            }
        } else {
            VStack(alignment: .leading, spacing: .sm) {
                StyledText("Source workspace", Typography.masthead)
                Text("Select a Source to inspect its Pipeline pin, configuration, corpus, and live execution state.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
                    .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(.xl)
        }
    }
}

private struct SourceDeploymentRow: View {
    let source: SourceDeployment
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            HStack(spacing: .xs) {
                Text(source.displayTitle)
                    .windexStyle(Typography.label)
                Spacer(minLength: 0)
                StatusBadge(source.status.activity.badgeStatus, word: source.status.activity.rawValue)
            }
            Text("\(source.pipeline.pipeline) @ \(source.pipeline.version)")
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
        }
        .padding(.vertical, .xxs)
        .accessibilityElement(children: .combine)
    }
}

private struct SourceDeploymentDetail: View {
    private enum Section: String, CaseIterable, Identifiable {
        case overview
        case settings
        case runs
        case activity
        case pipeline

        var id: Self { self }
        var title: String { rawValue.capitalized }
    }

    let source: SourceDeployment
    let openPipeline: () -> Void
    @State private var section: Section = .overview
    @State private var form: FormModel?
    @State private var isSaving = false
    @State private var saveError: String?
    @Environment(BackendSession.self) private var session
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(spacing: 0) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: .xs) {
                    StyledText(source.displayTitle, Typography.masthead)
                    Text(source.name)
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                }
                Spacer()
                StatusBadge(
                    source.status.activity.badgeStatus,
                    word: source.status.activity.rawValue)
            }
            .padding(.horizontal, .xl)
            .padding(.top, .lg)

            Picker("Source section", selection: $section) {
                ForEach(Section.allCases) {
                    Text($0.title).tag($0)
                }
            }
            .pickerStyle(.segmented)
            .padding(.horizontal, .xl)
            .padding(.vertical, .md)

            Hairline()

            ScrollView {
                VStack(alignment: .leading, spacing: .lg) {
                    sectionContent
                }
                .padding(.xl)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .task(id: source.configuration.valuesHash) {
            form = FormModel(
                params: source.configuration.fields,
                values: source.configuration.effectiveValues)
        }
    }

    @ViewBuilder
    private var sectionContent: some View {
        switch section {
        case .overview:
            if !source.description.isEmpty {
                Text(source.description)
                    .windexStyle(Typography.body)
                    .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
            }
            fieldGroup("Deployment") {
                dataRow("Pipeline", "\(source.pipeline.pipeline) @ \(source.pipeline.version)")
                dataRow("Origin", source.origin)
                dataRow("Generation", String(source.generation))
                dataRow("State namespace", source.stateNamespace)
                dataRow(
                    "Control",
                    source.archived ? "archived"
                        : source.paused ? "paused"
                        : source.enabled ? "enabled" : "disabled")
            }
            fieldGroup("Search identity") {
                dataRow("Search name", source.search.searchName)
                dataRow("ID prefix", source.search.idPrefix)
                dataRow("Collection", source.search.collectionKey)
                dataRow("Profile", source.search.searchProfile)
            }
            fieldGroup("Corpus") {
                dataRow("Searchable", source.status.counts.searchable.formatted())
                dataRow("Staged", source.status.counts.staged.formatted())
                dataRow(
                    "Pending embedding",
                    source.status.counts.pendingEmbedding.formatted())
                dataRow("Failed", source.status.counts.failed.formatted())
            }

        case .settings:
            HStack {
                Text(
                    source.configuration.isReady
                        ? "All required values are resolved."
                        : "\(source.configuration.missingRequired.count) required value(s) are missing."
                )
                .windexStyle(Typography.body)
                Spacer()
                StatusBadge(
                    source.configuration.isReady ? .healthy : .attention,
                    word: source.configuration.isReady ? "ready" : "incomplete")
            }
            if let form, !form.params.isEmpty {
                SchemaForm(model: form)
                Button(isSaving ? "Saving…" : "Save settings") {
                    Task {
                        isSaving = true
                        defer { isSaving = false }
                        do {
                            try await session.saveSourceSettings(
                                source.name, values: form.changes)
                            saveError = nil
                        } catch {
                            saveError = error.localizedDescription
                        }
                    }
                }
                .disabled(isSaving || !form.canSubmit)
                if let saveError {
                    Text(saveError)
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.rust)
                }
            } else {
                Text("This Source has no configurable parameters.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }

        case .runs:
            if let current = source.status.currentRun {
                runSummary("Current Run", current)
            }
            if let latest = source.status.latestRun,
               latest.id != source.status.currentRun?.id {
                runSummary("Latest Run", latest)
            }
            if source.status.currentRun == nil, source.status.latestRun == nil {
                Text("This Source has no Run history.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }

        case .activity:
            fieldGroup("Live state") {
                dataRow("Activity", source.status.activity.rawValue)
                dataRow("Next trigger", source.status.nextTrigger ?? "not scheduled")
                if let error = source.status.recentError {
                    Text(error)
                        .windexStyle(Typography.data)
                        .foregroundStyle(theme.palette.rust)
                        .textSelection(.enabled)
                }
            }
            Text(
                "Recent Run Events will appear here from the shared operational journal."
            )
            .windexStyle(Typography.body)
            .foregroundStyle(theme.palette.graphite)

        case .pipeline:
            fieldGroup("Pinned immutable revision") {
                dataRow("Pipeline", source.pipeline.pipeline)
                dataRow("Version", String(source.pipeline.version))
                dataRow("Semantic hash", source.pipeline.specHash)
                Button("Open Pipeline revision", action: openPipeline)
            }
        }
    }

    private func runSummary(
        _ title: String,
        _ run: SourceRunSummary
    ) -> some View {
        fieldGroup(title) {
            HStack {
                Text("Run \(run.id) · \(run.flow)")
                    .windexStyle(Typography.data)
                Spacer()
                StatusBadge(run.state.badgeStatus, word: run.state.rawValue)
            }
            if let progress = run.progress {
                ProgressView(value: progress)
            }
        }
    }

    private func fieldGroup<Content: View>(
        _ title: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText(title, Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            content()
        }
    }

    private func dataRow(_ label: String, _ value: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: .md) {
            Text(label)
                .windexStyle(Typography.label)
                .foregroundStyle(theme.palette.graphite)
                .frame(width: 130, alignment: .leading)
            Text(value)
                .windexStyle(Typography.data)
                .textSelection(.enabled)
            Spacer(minLength: 0)
        }
    }
}

private extension SourceActivityState {
    var badgeStatus: Status {
        switch self {
        case .idle, .succeeded:
            .healthy
        case .queued, .running:
            .running
        case .blocked, .paused:
            .attention
        case .failed, .cancelled, .archived:
            .fault
        }
    }
}
