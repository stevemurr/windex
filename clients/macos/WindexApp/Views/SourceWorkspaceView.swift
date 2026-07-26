import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class SourceUpgradeEditorModel {
    private(set) var preview: SourceUpgradePreviewWire?
    private(set) var candidateForm: FormModel?
    private(set) var previewedValues: [String: JSONValue] = [:]

    var hasUnpreviewedChanges: Bool {
        guard preview != nil, let candidateForm else { return false }
        return candidateForm.values != previewedValues
    }

    var canConfirm: Bool {
        guard let preview, preview.valid,
              preview.confirmationToken?.isEmpty == false,
              let candidateForm else { return false }
        return candidateForm.errors.isEmpty && !hasUnpreviewedChanges
    }

    var valuesForPreview: [String: JSONValue]? {
        candidateForm?.values
    }

    var confirmation: (
        version: Int,
        values: [String: JSONValue],
        token: String
    )? {
        guard canConfirm, let preview, let token = preview.confirmationToken else {
            return nil
        }
        return (preview.targetVersion, previewedValues, token)
    }

    @discardableResult
    func applyIfCurrent(
        _ response: SourceUpgradePreviewWire,
        parameters: [Param],
        requestedVersion: Int,
        selectedVersion: Int
    ) -> Bool {
        guard selectedVersion == requestedVersion,
              response.targetVersion == requestedVersion else {
            return false
        }
        let values = Self.object(response.candidate)
        preview = response
        previewedValues = values
        candidateForm = FormModel(params: parameters, values: values)
        return true
    }

    func reset() {
        preview = nil
        candidateForm = nil
        previewedValues = [:]
    }

    private static func object<T: Encodable>(
        _ value: T
    ) -> [String: JSONValue] {
        guard let data = try? JSONEncoder().encode(value),
              let result = try? JSONDecoder().decode(
                [String: JSONValue].self,
                from: data
              ) else { return [:] }
        return result
    }
}

/// The canonical Source surface. A Source deploys one immutable Pipeline
/// revision with origin, search identity, configuration, and runtime control.
struct SourcesView: View {
    @Bindable var appModel: AppModel
    @Environment(BackendSession.self) private var session
    @State private var selectedName: String?
    @State private var isCreating = false
    @State private var creationRevision: PipelineRevisionReference?
    @Environment(\.windexTheme) private var theme

    var body: some View {
        HSplitView {
            catalogue
                .frame(minWidth: 240, idealWidth: 280, maxWidth: 340)
            detail
                .frame(minWidth: 620)
        }
        .background(theme.palette.ink)
        .onChange(of: session.sources.sources) { _, sources in
            if selectedName == nil {
                selectedName = sources.first?.name
            } else if !sources.contains(where: { $0.name == selectedName }) {
                selectedName = sources.first?.name
            }
        }
        .onChange(of: appModel.selectedSourceName) { _, name in
            guard let name else { return }
            selectedName = name
            appModel.selectedSourceName = nil
        }
        .onChange(of: appModel.sourceCreationRevision) { _, reference in
            guard let reference else { return }
            creationRevision = reference
            isCreating = true
            appModel.sourceCreationRevision = nil
        }
        .task {
            if let name = appModel.selectedSourceName {
                selectedName = name
                appModel.selectedSourceName = nil
            }
            if let reference = appModel.sourceCreationRevision {
                creationRevision = reference
                isCreating = true
                appModel.sourceCreationRevision = nil
            }
        }
        .sheet(isPresented: $isCreating) {
            NewSourceSheet(initialRevision: creationRevision) { name in
                selectedName = name
            }
            .frame(minWidth: 680, minHeight: 720)
        }
    }

    private var catalogue: some View {
        VStack(spacing: 0) {
            HStack(alignment: .firstTextBaseline) {
                StyledText("Sources", Typography.masthead)
                Spacer()
                Button {
                    creationRevision = nil
                    isCreating = true
                } label: {
                    Label("New source", systemImage: "plus")
                }
                .disabled(session.pipelines.pipelines.isEmpty)
                .help("Deploy a published Pipeline revision as a Source.")
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
            Text("No Sources yet.")
                .windexStyle(Typography.label)
            Text(
                "Deploy a pinned Pipeline revision, then configure its origin, schedules, and searchable corpus."
            )
            .windexStyle(Typography.body)
            .foregroundStyle(theme.palette.graphite)
            .fixedSize(horizontal: false, vertical: true)
            Button("New Source") {
                creationRevision = nil
                isCreating = true
            }
                .disabled(session.pipelines.pipelines.isEmpty)
            if session.pipelines.pipelines.isEmpty {
                Button("Build a Pipeline first") {
                    appModel.selection = .pipelines
                }
            }
            Spacer()
        }
        .padding(.lg)
    }

    @ViewBuilder
    private var detail: some View {
        if let selectedName,
           let source = session.sources.sources.first(where: { $0.name == selectedName }) {
            SourceDeploymentDetail(
                source: source,
                openPipeline: {
                    appModel.openPipeline(source.pipeline)
                },
                openConsole: {
                    appModel.openConsole(
                        OperationalEventFilter(sourceName: source.name)
                    )
                },
                openRun: { id in
                    appModel.openRun(id)
                }
            )
        } else {
            VStack(alignment: .leading, spacing: .sm) {
                StyledText("Source workspace", Typography.masthead)
                Text(
                    "Select a Source to inspect its Pipeline pin, configuration, corpus, schedules, and execution state."
                )
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(.xl)
        }
    }
}

private struct NewSourceSheet: View {
    let initialRevision: PipelineRevisionReference?
    let onCreated: (String) -> Void
    @Environment(BackendSession.self) private var session
    @Environment(\.dismiss) private var dismiss
    @Environment(\.windexTheme) private var theme

    @State private var name = ""
    @State private var title = ""
    @State private var description = ""
    @State private var pipelineName = ""
    @State private var pipelineVersion = 0
    @State private var originJSON = #"{"ingress":"push"}"#
    @State private var valuesForm: FormModel?
    @State private var searchName = ""
    @State private var idPrefix = ""
    @State private var collectionKey = ""
    @State private var searchProfile = "default"
    @State private var stateNamespace = ""
    @State private var includeInAll = true
    @State private var enabled = true
    @State private var isCreating = false
    @State private var errorMessage: String?
    @State private var validationIssues: [Components.Schemas.ValidationIssueModel] = []

    private var eligiblePipelines: [PipelineSummary] {
        session.pipelines.pipelines.filter { pipeline in
            session.pipelines.revisions[pipeline.name]?.contains {
                $0.sourceCapability.capable
            } == true
        }
    }

    private var eligibleRevisions: [PipelineRevision] {
        (session.pipelines.revisions[pipelineName] ?? [])
            .filter(\.sourceCapability.capable)
    }

    private var selectedRevision: PipelineRevision? {
        session.pipelines.revisions[pipelineName]?
            .first { $0.reference.version == pipelineVersion }
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                StyledText("New Source", Typography.masthead)
                Spacer()
                Button("Cancel") { dismiss() }
            }
            .padding(.lg)
            Hairline()

            ScrollView {
                VStack(alignment: .leading, spacing: .lg) {
                    Text(
                        "A Source is the searchable deployment rail around one immutable Pipeline revision."
                    )
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)

                    Group {
                        TextField("Source name", text: $name)
                        TextField("Display title", text: $title)
                        TextField("Description", text: $description, axis: .vertical)
                    }
                    .textFieldStyle(.roundedBorder)

                    HStack {
                        Picker("Pipeline", selection: $pipelineName) {
                            ForEach(eligiblePipelines) {
                                Text($0.displayTitle).tag($0.name)
                            }
                        }
                        Picker("Revision", selection: $pipelineVersion) {
                            ForEach(eligibleRevisions, id: \.reference) {
                                Text("v\($0.reference.version)").tag($0.reference.version)
                            }
                        }
                    }
                    .onChange(of: pipelineName) {
                        pipelineVersion = eligibleRevisions.first?.reference.version ?? 0
                        seedValuesForm()
                    }
                    .onChange(of: pipelineVersion) {
                        seedValuesForm()
                    }

                    jsonEditor(
                        "Origin",
                        help: #"Use {"ingress":"push"} for push ingestion, or the origin object expected by the Pipeline."#,
                        text: $originJSON
                    )
                    VStack(alignment: .leading, spacing: .sm) {
                        Text("Pipeline values")
                            .windexStyle(Typography.label)
                        Text(
                            "This form is generated from the selected immutable revision. Secret fields store references only."
                        )
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                        if let valuesForm, !valuesForm.params.isEmpty {
                            SchemaForm(
                                model: valuesForm,
                                configuredSecretReferences: session.sources.configuredSecrets
                            )
                        } else {
                            Text("This revision declares no Source parameters.")
                                .windexStyle(Typography.body)
                                .foregroundStyle(theme.palette.graphite)
                        }
                        if !session.sources.configuredSecrets.isEmpty {
                            Text(
                                "Configured secrets: "
                                    + session.sources.configuredSecrets.joined(separator: ", ")
                            )
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.graphite)
                        }
                    }

                    DisclosureGroup("Search identity and control") {
                        VStack(alignment: .leading, spacing: .sm) {
                            TextField("Search name (defaults to Source name)", text: $searchName)
                            TextField("ID prefix (defaults to name:)", text: $idPrefix)
                            TextField("Collection key", text: $collectionKey)
                            TextField("Search profile", text: $searchProfile)
                            TextField("State namespace", text: $stateNamespace)
                            Toggle("Include in all-source search", isOn: $includeInAll)
                            Toggle("Enabled", isOn: $enabled)
                        }
                        .textFieldStyle(.roundedBorder)
                        .padding(.top, .sm)
                    }

                    if let errorMessage {
                        Text(errorMessage)
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.rust)
                            .textSelection(.enabled)
                    }
                    ForEach(validationIssues, id: \.code) { issue in
                        HStack(alignment: .top, spacing: .xs) {
                            StatusBadge(
                                issue.severity == .error ? .fault : .attention,
                                word: issue.severity.rawValue
                            )
                            VStack(alignment: .leading, spacing: .xxs) {
                                Text(issue.path)
                                    .windexStyle(Typography.dataSM)
                                Text(issue.message)
                                    .windexStyle(Typography.body)
                            }
                        }
                    }
                }
                .padding(.xl)
            }

            Hairline()
            HStack {
                Spacer()
                Button(isCreating ? "Creating…" : "Validate and create") {
                    Task { await create() }
                }
                .buttonStyle(.borderedProminent)
                .disabled(
                    isCreating || name.trimmingCharacters(in: .whitespaces).isEmpty
                        || pipelineName.isEmpty || pipelineVersion == 0
                        || valuesForm?.errors.isEmpty == false
                )
            }
            .padding(.lg)
        }
        .background(theme.palette.ink)
        .onAppear {
            if let initialRevision,
               session.pipelines.revisions[initialRevision.pipeline]?
                .contains(where: {
                    $0.reference.version == initialRevision.version
                        && $0.sourceCapability.capable
                }) == true {
                pipelineName = initialRevision.pipeline
                pipelineVersion = initialRevision.version
            } else if pipelineName.isEmpty,
                      let first = eligiblePipelines.first {
                pipelineName = first.name
                pipelineVersion = session.pipelines.revisions[first.name]?
                    .first(where: \.sourceCapability.capable)?
                    .reference.version ?? 0
            }
            seedValuesForm()
        }
    }

    private func jsonEditor(
        _ title: String,
        help: String,
        text: Binding<String>
    ) -> some View {
        VStack(alignment: .leading, spacing: .xs) {
            Text(title).windexStyle(Typography.label)
            Text(help)
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
            TextEditor(text: text)
                .font(.system(.body, design: .monospaced))
                .frame(minHeight: 84)
                .padding(.xs)
                .background(theme.palette.plate)
                .clipShape(RoundedRectangle(cornerRadius: 6))
        }
    }

    private func create() async {
        isCreating = true
        defer { isCreating = false }
        do {
            let canonical = canonicalName
            let request = SourceCreateRequest(
                name: canonical,
                title: title,
                description: description,
                origin: try decodeObject(originJSON),
                pipelineName: pipelineName,
                pipelineVersion: pipelineVersion,
                values: valuesForm?.values ?? [:],
                searchName: searchName.isEmpty ? canonical : searchName,
                idPrefix: idPrefix.isEmpty ? "\(canonical):" : idPrefix,
                collectionKey: collectionKey.isEmpty ? canonical : collectionKey,
                searchProfile: searchProfile.isEmpty ? "default" : searchProfile,
                includeInAll: includeInAll,
                stateNamespace: stateNamespace.isEmpty ? canonical : stateNamespace,
                enabled: enabled
            )
            let report = try await session.validateSource(request)
            validationIssues = report.issues
            guard report.valid else {
                errorMessage = "Resolve the deployment validation failures before creation."
                return
            }
            try await session.createSource(request)
            onCreated(canonical)
            dismiss()
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func seedValuesForm() {
        guard let selectedRevision else {
            valuesForm = nil
            return
        }
        valuesForm = FormModel(params: selectedRevision.spec.parameters)
        validationIssues = selectedRevision.sourceCapability.issues.map { issue in
            Components.Schemas.ValidationIssueModel(
                code: issue.code,
                message: issue.message,
                path: issue.path,
                severity: issue.severity == .error ? .error : .warning
            )
        }
    }

    private var canonicalName: String {
        name.lowercased()
            .replacingOccurrences(
                of: #"[^a-z0-9_-]+"#,
                with: "-",
                options: .regularExpression
            )
            .trimmingCharacters(in: CharacterSet(charactersIn: "-"))
    }

    private func decodeObject(_ value: String) throws -> [String: JSONValue] {
        guard let data = value.data(using: .utf8) else {
            throw SourceFormError.invalidJSON
        }
        do {
            return try JSONDecoder().decode([String: JSONValue].self, from: data)
        } catch {
            throw SourceFormError.json(error.localizedDescription)
        }
    }
}

private enum SourceFormError: LocalizedError {
    case invalidJSON
    case json(String)
    var errorDescription: String? {
        switch self {
        case .invalidJSON: "Enter a JSON object."
        case .json(let message): "The JSON object is invalid: \(message)"
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
                StatusBadge(
                    source.status.activity.badgeStatus,
                    word: source.status.activity.rawValue
                )
            }
            Text("\(source.pipeline.pipeline) @ \(source.pipeline.version)")
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
        }
        .padding(.vertical, .xxs)
        .accessibilityElement(children: .combine)
    }
}

private enum SourceLifecycleConfirmation: Identifiable {
    case reset(Components.Schemas.ResetPreviewResponse)
    case archive

    var id: String {
        switch self {
        case .reset: "reset"
        case .archive: "archive"
        }
    }
}

private struct TriggerEditorItem: Identifiable {
    let trigger: SourceTriggerWire?
    var id: String { trigger.map { "edit-\($0.id)" } ?? "new" }
}

private struct SourceDeploymentDetail: View {
    private enum Section: String, CaseIterable, Identifiable {
        case overview
        case settings
        case runs
        case activity
        case ingestion
        case pipeline

        var id: Self { self }
        var title: String { rawValue.capitalized }
    }

    let source: SourceDeployment
    let openPipeline: () -> Void
    let openConsole: () -> Void
    let openRun: (Int) -> Void
    @State private var section: Section = .overview
    @State private var isMutating = false
    @State private var actionError: String?
    @State private var settingsConflict: SettingsConflict?
    @State private var confirmation: SourceLifecycleConfirmation?
    @State private var triggerEditor: TriggerEditorItem?
    @State private var isUpgrading = false
    @State private var ingestID = ""
    @State private var ingestURL = ""
    @State private var ingestTitle = ""
    @State private var ingestText = ""
    @State private var ingestConversationID = ""
    @State private var ingestChunkIndex = 0
    @State private var ingestMessageStart = 0
    @State private var ingestMessageEnd = 1
    @State private var isConfirmingMemoryDelete = false
    @Environment(BackendSession.self) private var session
    @Environment(\.windexTheme) private var theme

    private struct SettingsConflict {
        let changes: [String: JSONValue]
        let server: SettingsScope
    }

    var body: some View {
        VStack(spacing: 0) {
            header
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
                    if let actionError {
                        Text(actionError)
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.rust)
                            .textSelection(.enabled)
                    }
                }
                .padding(.xl)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .task(id: source.configuration.valuesHash) {
            settingsConflict = nil
        }
        .sheet(item: $confirmation) { value in
            LifecycleConfirmationSheet(
                source: source,
                confirmation: value,
                isMutating: $isMutating,
                errorMessage: $actionError
            )
        }
        .sheet(item: $triggerEditor) { item in
            TriggerSheet(source: source, trigger: item.trigger)
                .frame(minWidth: 480, minHeight: 360)
        }
        .sheet(isPresented: $isUpgrading) {
            SourceUpgradeSheet(source: source)
                .frame(minWidth: 680, minHeight: 720)
        }
        .confirmationDialog(
            "Delete memory conversation?",
            isPresented: $isConfirmingMemoryDelete,
            titleVisibility: .visible
        ) {
            Button("Delete conversation", role: .destructive) {
                Task { await deleteMemoryConversation() }
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text(
                "A full empty push will remove every indexed chunk in conversation "
                    + "“\(memoryConversationID)”."
            )
        }
    }

    private var header: some View {
        VStack(spacing: .md) {
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
                    word: source.status.activity.rawValue
                )
            }
            HStack(spacing: .sm) {
                Button("Run latest") {
                    Task {
                        await perform { try await session.runLatest(source: source.name) }
                    }
                }
                .help("Runs the Source’s current revision and configuration.")
                Button(source.paused ? "Resume" : "Pause") {
                    Task {
                        await perform {
                            if source.paused {
                                try await session.resumeSource(source.name)
                            } else {
                                try await session.pauseSource(
                                    source.name,
                                    reason: "Paused from the macOS app"
                                )
                            }
                        }
                    }
                }
                Button(source.enabled ? "Disable" : "Enable") {
                    Task {
                        await perform {
                            try await session.setSourceEnabled(
                                source.name,
                                enabled: !source.enabled
                            )
                        }
                    }
                }
                .help(
                    source.enabled
                        ? "Disable automatic and manual Source execution without pausing active work."
                        : "Enable Source execution. Pause state remains independent."
                )
                if session.pipelines.revisions[source.pipeline.pipeline]?.contains(where: {
                    $0.reference.version != source.pipeline.version
                        && $0.sourceCapability.capable
                }) == true {
                    Button("Upgrade Source…") {
                        isUpgrading = true
                    }
                }
                Menu {
                    Button("Reset corpus and state", role: .destructive) {
                        Task {
                            do {
                                confirmation = .reset(
                                    try await session.previewSourceReset(source.name)
                                )
                            } catch {
                                actionError = error.localizedDescription
                            }
                        }
                    }
                    Button("Archive Source", role: .destructive) {
                        confirmation = .archive
                    }
                } label: {
                    Label("More", systemImage: "ellipsis.circle")
                }
                .menuStyle(.borderlessButton)
                Spacer()
                if isMutating {
                    ProgressView().controlSize(.small)
                }
            }
        }
        .padding(.horizontal, .xl)
        .padding(.top, .lg)
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
                dataRow("Enabled", source.enabled ? "yes" : "no")
                dataRow("Paused", source.paused ? "yes" : "no")
                dataRow("Generation", String(source.generation))
                dataRow("State namespace", source.stateNamespace)
                dataRow(
                    "Control",
                    source.archived ? "archived"
                        : source.paused ? "paused"
                        : source.enabled ? "enabled" : "disabled"
                )
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
                dataRow("Pending embedding", source.status.counts.pendingEmbedding.formatted())
                dataRow("Failed", source.status.counts.failed.formatted())
            }

        case .settings:
            settingsSection

        case .runs:
            let sourceRuns = session.runs.runs(for: source.name)
            if sourceRuns.isEmpty {
                Text("This Source has no Run history.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            } else {
                ForEach(sourceRuns) { run in
                    runSummary(run)
                }
                if session.runs.sourceHistoryHasMore[source.name] == true {
                    Button("Load older Runs") {
                        Task {
                            await session.loadMoreSourceRuns(source.name)
                        }
                    }
                }
            }

        case .activity:
            activitySection

        case .ingestion:
            ingestionSection

        case .pipeline:
            fieldGroup("Pinned immutable revision") {
                dataRow("Pipeline", source.pipeline.pipeline)
                dataRow("Version", String(source.pipeline.version))
                dataRow("Semantic hash", source.pipeline.specHash)
                Button("Open Pipeline revision", action: openPipeline)
            }
        }
    }

    @ViewBuilder
    private var settingsSection: some View {
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
                word: source.configuration.isReady ? "ready" : "incomplete"
            )
        }
        if let activeForm = session.sourceSettingsDrafts.form(for: source.name),
           !activeForm.params.isEmpty {
            SchemaForm(
                model: activeForm,
                configuredSecretReferences: session.sources.configuredSecrets
            )
            Button(isMutating ? "Saving…" : "Save settings") {
                Task { await saveSettings(activeForm.changes) }
            }
            .disabled(isMutating || !activeForm.canSubmit)

            if let conflict = settingsConflict {
                VStack(alignment: .leading, spacing: .sm) {
                    Text("Settings changed on the server")
                        .windexStyle(Typography.label)
                        .foregroundStyle(theme.palette.rust)
                    ForEach(conflict.changes.keys.sorted(), id: \.self) { key in
                        let serverValue = conflict.server.fields.first {
                            $0.key == key
                        }?.value?.displayString ?? "unset"
                        Text(
                            "\(key): server \(serverValue) · yours "
                                + (conflict.changes[key]?.displayString ?? "unset")
                        )
                        .windexStyle(Typography.dataSM)
                    }
                    HStack {
                        Button("Reload server values") {
                            session.sourceSettingsDrafts.adopt(conflict.server)
                            settingsConflict = nil
                        }
                        Button("Reapply my changes") {
                            Task { await saveSettings(conflict.changes) }
                        }
                        .buttonStyle(.borderedProminent)
                    }
                }
                .padding(.md)
                .background(theme.palette.plate)
            }
        } else {
            Text("This Source has no configurable parameters.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
        }
    }

    @ViewBuilder
    private var activitySection: some View {
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
        HStack {
            StyledText("Triggers", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            Spacer()
            Button("Add trigger") {
                triggerEditor = TriggerEditorItem(trigger: nil)
            }
        }
        let triggers = session.sources.triggers[source.name] ?? []
        if triggers.isEmpty {
            Text("No triggers. Runs can still be started manually.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
        }
        ForEach(triggers, id: \.id) { trigger in
            HStack {
                VStack(alignment: .leading, spacing: .xxs) {
                    Text("\(trigger.triggerType) · \(trigger.flowName)")
                        .windexStyle(Typography.label)
                    Text(trigger.nextFireAt ?? "not armed")
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.graphite)
                }
                Spacer()
                Button("Edit") {
                    triggerEditor = TriggerEditorItem(trigger: trigger)
                }
                .buttonStyle(.borderless)
                Toggle(
                    "Enabled",
                    isOn: Binding(
                        get: { trigger.enabled },
                        set: { enabled in
                            Task {
                                await perform {
                                    try await session.setTriggerEnabled(
                                        source: source.name,
                                        id: trigger.id,
                                        enabled: enabled
                                    )
                                }
                            }
                        }
                    )
                )
                .labelsHidden()
                Button(role: .destructive) {
                    Task {
                        await perform {
                            try await session.deleteTrigger(
                                source: source.name,
                                id: trigger.id
                            )
                        }
                    }
                } label: {
                    Image(systemName: "trash")
                }
                .buttonStyle(.borderless)
            }
        }
        let sourceEvents = session.logs.allEvents
            .filter { $0.sourceName == source.name }
            .suffix(12)
        if !sourceEvents.isEmpty {
            Hairline()
            HStack {
                StyledText("Recent Events", Typography.eyebrow)
                    .foregroundStyle(theme.palette.graphite)
                Spacer()
                Button("Open in Console", action: openConsole)
            }
            ForEach(sourceEvents) { event in
                Text("\(event.event) · \(event.message)")
                    .windexStyle(Typography.dataSM)
            }
        }
    }

    @ViewBuilder
    private var ingestionSection: some View {
        if let ingress = source.ingress {
            fieldGroup("Push contract") {
                dataRow("Endpoint", pushURL(ingress.path))
                dataRow("Authentication", ingress.authenticationRequired ? "Bearer write token" : "none")
                dataRow("Maximum documents", ingress.maxDocuments.formatted())
                dataRow("Maximum text bytes", ingress.maxTextBytes.formatted())
                dataRow(
                    "Modes",
                    isMemorySource ? "full" : ingress.modes.joined(separator: ", ")
                )
            }
            Text("curl example")
                .windexStyle(Typography.label)
            Text(curlExample(ingress))
                .font(.system(.callout, design: .monospaced))
                .textSelection(.enabled)
                .padding(.md)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(theme.palette.plate)

            Hairline()
            if isMemorySource {
                memoryIngestionForm
            } else {
                genericIngestionForm(ingress)
            }
        } else {
            Text(
                "This Pipeline revision does not expose push ingestion. Add a push.docs input module and publish a new revision to enable it."
            )
            .windexStyle(Typography.body)
            .foregroundStyle(theme.palette.graphite)
            .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
        }
    }

    private var memoryConversationID: String {
        ingestConversationID.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var isMemorySource: Bool {
        source.search.searchName == "memory"
    }

    private var memoryIngestionForm: some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Replace one conversation", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            Text(
                "Memory ingestion always uses full replacement. This diagnostic form "
                    + "replaces one conversation with one chunk; production clients "
                    + "should send every chunk for that conversation in the same request."
            )
            .windexStyle(Typography.body)
            .foregroundStyle(theme.palette.graphite)
            .frame(maxWidth: Layout.proseMeasure, alignment: .leading)

            TextField("Conversation ID", text: $ingestConversationID)
            HStack {
                TextField(
                    "Chunk index",
                    value: $ingestChunkIndex,
                    format: .number
                )
                TextField(
                    "Message range start",
                    value: $ingestMessageStart,
                    format: .number
                )
                TextField(
                    "Message range end",
                    value: $ingestMessageEnd,
                    format: .number
                )
            }
            TextField("Title", text: $ingestTitle)
            TextEditor(text: $ingestText)
                .frame(minHeight: 120)
                .padding(.xs)
                .background(theme.palette.plate)

            HStack {
                Button("Replace conversation") {
                    Task { await replaceMemoryConversation() }
                }
                .buttonStyle(.borderedProminent)
                .disabled(
                    memoryConversationID.isEmpty
                        || ingestText.isEmpty
                        || ingestChunkIndex < 0
                        || ingestMessageStart < 0
                        || ingestMessageEnd < ingestMessageStart
                        || isMutating
                )

                Button("Delete conversation", role: .destructive) {
                    isConfirmingMemoryDelete = true
                }
                .disabled(memoryConversationID.isEmpty || isMutating)
            }
        }
    }

    private func genericIngestionForm(_ ingress: SourceIngress) -> some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Send one document", Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            TextField("Document ID", text: $ingestID)
            TextField("URL", text: $ingestURL)
            TextField("Title", text: $ingestTitle)
            TextEditor(text: $ingestText)
                .frame(minHeight: 120)
                .padding(.xs)
                .background(theme.palette.plate)
            Button("Queue ingestion") {
                Task {
                    await perform {
                        try await session.ingest(
                            [
                                IngestDocument(
                                    id: ingestID,
                                    url: ingestURL,
                                    text: ingestText,
                                    title: ingestTitle
                                )
                            ],
                            source: source.name,
                            mode: ingress.modes.first ?? "delta",
                            idempotencyKey: UUID().uuidString
                        )
                        ingestID = ""
                        ingestURL = ""
                        ingestTitle = ""
                        ingestText = ""
                    }
                }
            }
            .buttonStyle(.borderedProminent)
            .disabled(
                ingestID.isEmpty
                    || ingestURL.isEmpty
                    || ingestText.isEmpty
                    || isMutating
            )
        }
    }

    private func replaceMemoryConversation() async {
        await perform {
            let chunk = try MemoryConversationChunk(
                chunkIndex: ingestChunkIndex,
                messageRangeStart: ingestMessageStart,
                messageRangeEnd: ingestMessageEnd,
                text: ingestText,
                title: ingestTitle
            )
            let batch = try MemoryIngestBatch.replacement(
                conversationID: memoryConversationID,
                chunks: [chunk]
            )
            try await session.ingest(
                batch.documents,
                source: source.name,
                mode: batch.mode,
                partition: batch.partition,
                idempotencyKey: UUID().uuidString
            )
            resetMemoryIngestionForm()
        }
    }

    private func deleteMemoryConversation() async {
        await perform {
            let batch = try MemoryIngestBatch.deletion(
                conversationID: memoryConversationID
            )
            try await session.ingest(
                batch.documents,
                source: source.name,
                mode: batch.mode,
                partition: batch.partition,
                idempotencyKey: UUID().uuidString
            )
            resetMemoryIngestionForm()
        }
    }

    private func resetMemoryIngestionForm() {
        ingestConversationID = ""
        ingestChunkIndex = 0
        ingestMessageStart = 0
        ingestMessageEnd = 1
        ingestTitle = ""
        ingestText = ""
    }

    private func saveSettings(_ changes: [String: JSONValue]) async {
        await perform {
            do {
                try await session.saveSourceSettings(source.name, values: changes)
                settingsConflict = nil
            } catch let error as WindexError {
                guard case .preconditionFailed = error else { throw error }
                let response = try await session.sourceSettings(source.name)
                settingsConflict = SettingsConflict(
                    changes: changes,
                    server: try response.settingsScope()
                )
            }
        }
    }

    private func perform(_ action: () async throws -> Void) async {
        isMutating = true
        defer { isMutating = false }
        do {
            try await action()
            actionError = nil
        } catch {
            actionError = error.localizedDescription
        }
    }

    private func pushURL(_ path: String) -> String {
        if path.hasPrefix("http://") || path.hasPrefix("https://") { return path }
        return session.backend.profile.displayAddress + (path.hasPrefix("/") ? path : "/\(path)")
    }

    private func curlExample(_ ingress: SourceIngress) -> String {
        var headers = [
            #"-H 'Content-Type: application/json'"#,
            #"-H 'Idempotency-Key: <unique-batch-id>'"#
        ]
        if ingress.authenticationRequired {
            headers.append(#"-H 'Authorization: Bearer <write-token>'"#)
        }
        let body = if isMemorySource {
            #"{"schema_version":"windex.ingest/1","mode":"full","partition":"<conversation-id>","documents":[{"id":"<conversation-id>/00000","url":"llmchat://chat/<conversation-id>?chunk=0","title":"Example conversation","text":"Searchable conversation text","fields":{"conversation_id":"<conversation-id>","chunk_index":0,"message_range":[0,1]}}]}"#
        } else {
            #"{"schema_version":"windex.ingest/1","mode":"\#(ingress.modes.first ?? "delta")","documents":[{"id":"doc-1","url":"https://example.test/doc-1","title":"Example","text":"Searchable text"}]}"#
        }
        return """
        curl -X POST '\(pushURL(ingress.path))' \\
          \(headers.joined(separator: " \\\n  ")) \\
          --data '\(body)'
        """
    }

    private func runSummary(_ run: SourceRunSummary) -> some View {
        Button {
            openRun(run.id)
        } label: {
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
        .buttonStyle(.plain)
        .padding(.sm)
        .background(theme.palette.plate)
        .accessibilityLabel(
            "Open Run \(run.id), \(run.flow), \(run.state.rawValue)"
        )
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
                .frame(width: 140, alignment: .leading)
            Text(value)
                .windexStyle(Typography.data)
                .textSelection(.enabled)
            Spacer(minLength: 0)
        }
    }
}

private struct LifecycleConfirmationSheet: View {
    let source: SourceDeployment
    let confirmation: SourceLifecycleConfirmation
    @Binding var isMutating: Bool
    @Binding var errorMessage: String?
    @Environment(BackendSession.self) private var session
    @Environment(\.dismiss) private var dismiss
    @Environment(\.windexTheme) private var theme
    @State private var typedConfirmation = ""

    var body: some View {
        VStack(alignment: .leading, spacing: .lg) {
            switch confirmation {
            case .reset(let preview):
                StyledText("Reset \(source.displayTitle)?", Typography.setLG)
                Text(
                    "This queues generation \(preview.generation + 1), replacing "
                        + "\(preview.documents) documents and \(preview.stateUnits) state units. "
                        + "\(preview.outstandingTasks) outstanding tasks are affected."
                )
                TextField(
                    "Type \(source.name) to confirm",
                    text: $typedConfirmation
                )
                .textFieldStyle(.roundedBorder)
                confirmationButtons(
                    "Queue reset",
                    role: .destructive,
                    enabled: typedConfirmation == source.name
                ) {
                    try await session.resetSource(
                        source.name,
                        confirmationToken: preview.confirmationToken
                    )
                }
            case .archive:
                StyledText("Archive \(source.displayTitle)?", Typography.setLG)
                Text("The Source leaves active lists and can no longer be run or scheduled.")
                confirmationButtons("Archive Source", role: .destructive) {
                    try await session.archiveSource(source.name)
                }
            }
            if let errorMessage {
                Text(errorMessage)
                    .foregroundStyle(theme.palette.rust)
                    .textSelection(.enabled)
            }
        }
        .padding(.xl)
        .frame(minWidth: 480, minHeight: 240, alignment: .topLeading)
        .background(theme.palette.ink)
    }

    private func confirmationButtons(
        _ label: String,
        role: ButtonRole?,
        enabled: Bool = true,
        action: @escaping () async throws -> Void
    ) -> some View {
        HStack {
            Button("Cancel") { dismiss() }
            Spacer()
            Button(label, role: role) {
                Task {
                    isMutating = true
                    defer { isMutating = false }
                    do {
                        try await action()
                        errorMessage = nil
                        dismiss()
                    } catch {
                        errorMessage = error.localizedDescription
                    }
                }
            }
            .buttonStyle(.borderedProminent)
            .disabled(isMutating || !enabled)
        }
    }
}

private struct SourceUpgradeSheet: View {
    let source: SourceDeployment
    @Environment(BackendSession.self) private var session
    @Environment(\.dismiss) private var dismiss
    @Environment(\.windexTheme) private var theme

    @State private var targetVersion = 0
    @State private var editor = SourceUpgradeEditorModel()
    @State private var isWorking = false
    @State private var errorMessage: String?

    private var eligibleRevisions: [PipelineRevision] {
        (session.pipelines.revisions[source.pipeline.pipeline] ?? [])
            .filter {
                $0.reference.version != source.pipeline.version
                    && $0.sourceCapability.capable
            }
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                VStack(alignment: .leading, spacing: .xs) {
                    StyledText("Upgrade Source", Typography.setLG)
                    Text("\(source.pipeline.pipeline) @ \(source.pipeline.version)")
                        .windexStyle(Typography.data)
                        .foregroundStyle(theme.palette.graphite)
                }
                Spacer()
                Button("Close") { dismiss() }
            }
            .padding(.lg)
            Hairline()

            ScrollView {
                VStack(alignment: .leading, spacing: .lg) {
                    HStack {
                        Picker("Target revision", selection: $targetVersion) {
                            ForEach(eligibleRevisions, id: \.reference) { revision in
                                Text(
                                    "v\(revision.reference.version)"
                                        + (revision.note.isEmpty ? "" : " · \(revision.note)")
                                )
                                .tag(revision.reference.version)
                            }
                        }
                        .disabled(isWorking)
                        Button(
                            isWorking
                                ? "Previewing…"
                                : (editor.preview == nil
                                    ? "Preview"
                                    : "Re-preview candidate")
                        ) {
                            Task {
                                await loadPreview(
                                    values: editor.valuesForPreview
                                )
                            }
                        }
                        .disabled(isWorking || targetVersion == 0)
                    }

                    if let preview = editor.preview {
                        HStack {
                            StatusBadge(
                                preview.valid ? .healthy : .attention,
                                word: preview.valid ? "valid" : "needs values"
                            )
                            Text(
                                "v\(preview.fromVersion) → v\(preview.targetVersion) · "
                                    + String(preview.targetHash.prefix(12))
                            )
                            .windexStyle(Typography.dataSM)
                        }

                        previewDictionary("Retained", object(preview.retained))
                        previewDictionary("Defaulted", object(preview.defaulted))
                        previewDictionary("Clamped", object(preview.clamped))
                        previewList("Removed", preview.removed)
                        previewList("Missing", preview.missing)
                        previewList(
                            "Install-stage changes",
                            preview.installStageChanged
                        )
                        previewDictionary("State impact", object(preview.stateImpact))

                        if !preview.issues.isEmpty {
                            Hairline()
                            StyledText("Validation issues", Typography.eyebrow)
                                .foregroundStyle(theme.palette.graphite)
                            ForEach(
                                Array(preview.issues.enumerated()),
                                id: \.offset
                            ) { _, issue in
                                VStack(alignment: .leading, spacing: .xxs) {
                                    HStack {
                                        StatusBadge(
                                            issue.severity == .error
                                                ? .fault
                                                : .attention,
                                            word: issue.severity.rawValue
                                        )
                                        Text(issue.code)
                                            .windexStyle(Typography.dataSM)
                                    }
                                    Text(issue.path)
                                        .windexStyle(Typography.dataSM)
                                        .foregroundStyle(theme.palette.graphite)
                                    Text(issue.message)
                                        .windexStyle(Typography.body)
                                }
                            }
                        }

                        Hairline()
                        StyledText("Candidate configuration", Typography.eyebrow)
                            .foregroundStyle(theme.palette.graphite)
                        if let candidateForm = editor.candidateForm,
                           !candidateForm.params.isEmpty {
                            SchemaForm(
                                model: candidateForm,
                                configuredSecretReferences: session.sources.configuredSecrets
                            )
                            HStack {
                                Button("Re-preview edited candidate") {
                                    Task {
                                        await loadPreview(
                                            values: candidateForm.values
                                        )
                                    }
                                }
                                .disabled(
                                    isWorking || !editor.hasUnpreviewedChanges
                                )
                                if editor.hasUnpreviewedChanges {
                                    StatusBadge(.attention, word: "edited")
                                }
                            }
                        } else {
                            Text("The target revision declares no parameters.")
                                .windexStyle(Typography.body)
                        }

                        if let candidateForm = editor.candidateForm,
                           !candidateForm.errors.isEmpty {
                            ForEach(candidateForm.errors, id: \.param.key) { issue in
                                Text("\(issue.param.key): \(issue.message)")
                                    .windexStyle(Typography.dataSM)
                                    .foregroundStyle(theme.palette.rust)
                            }
                        }

                        if editor.hasUnpreviewedChanges {
                            Text(
                                "Re-preview the edited candidate before confirming. "
                                    + "Only the latest server-validated candidate and its matching "
                                    + "confirmation token can be submitted."
                            )
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.amber)
                        }
                    }

                    if let errorMessage {
                        Text(errorMessage)
                            .windexStyle(Typography.body)
                            .foregroundStyle(theme.palette.rust)
                            .textSelection(.enabled)
                    }
                }
                .padding(.xl)
            }

            Hairline()
            HStack {
                Spacer()
                Button(isWorking ? "Upgrading…" : "Confirm upgrade") {
                    Task { await confirm() }
                }
                .buttonStyle(.borderedProminent)
                .disabled(isWorking || !editor.canConfirm)
            }
            .padding(.lg)
        }
        .background(theme.palette.ink)
        .onAppear {
            targetVersion = eligibleRevisions.first?.reference.version ?? 0
        }
        .onChange(of: targetVersion) {
            editor.reset()
            errorMessage = nil
        }
    }

    private func loadPreview(values: [String: JSONValue]? = nil) async {
        let requestedVersion = targetVersion
        let requestedParameters = eligibleRevisions.first {
            $0.reference.version == requestedVersion
        }?.spec.parameters ?? []
        isWorking = true
        defer { isWorking = false }
        do {
            let value = try await session.previewSourceUpgrade(
                source.name,
                version: requestedVersion,
                values: values
            )
            guard editor.applyIfCurrent(
                value,
                parameters: requestedParameters,
                requestedVersion: requestedVersion,
                selectedVersion: targetVersion
            ) else { return }
            errorMessage = nil
        } catch {
            guard targetVersion == requestedVersion else { return }
            errorMessage = error.localizedDescription
        }
    }

    private func confirm() async {
        guard let confirmation = editor.confirmation else { return }
        isWorking = true
        defer { isWorking = false }
        do {
            try await session.upgradeSource(
                source.name,
                version: confirmation.version,
                values: confirmation.values,
                confirmationToken: confirmation.token
            )
            dismiss()
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func object<T: Encodable>(_ value: T) -> [String: JSONValue] {
        guard let data = try? JSONEncoder().encode(value),
              let result = try? JSONDecoder().decode(
                [String: JSONValue].self,
                from: data
              ) else { return [:] }
        return result
    }

    @ViewBuilder
    private func previewDictionary(
        _ title: String,
        _ values: [String: JSONValue]
    ) -> some View {
        VStack(alignment: .leading, spacing: .xs) {
            StyledText(title, Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            if values.isEmpty {
                Text("None")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            } else {
                ForEach(values.keys.sorted(), id: \.self) { key in
                    Text("\(key): \(values[key]?.displayString ?? "null")")
                        .windexStyle(Typography.dataSM)
                        .textSelection(.enabled)
                }
            }
        }
    }

    @ViewBuilder
    private func previewList(_ title: String, _ values: [String]) -> some View {
        VStack(alignment: .leading, spacing: .xs) {
            StyledText(title, Typography.eyebrow)
                .foregroundStyle(theme.palette.graphite)
            Text(values.isEmpty ? "None" : values.joined(separator: ", "))
                .windexStyle(Typography.dataSM)
                .foregroundStyle(values.isEmpty ? theme.palette.graphite : theme.palette.paper)
        }
    }
}

private struct TriggerSheet: View {
    let source: SourceDeployment
    let trigger: SourceTriggerWire?
    @Environment(BackendSession.self) private var session
    @Environment(\.dismiss) private var dismiss
    @Environment(\.windexTheme) private var theme
    @State private var flow = ""
    @State private var kind = "interval"
    @State private var intervalSeconds = "3600"
    @State private var cron = "0 * * * *"
    @State private var timezone = "UTC"
    @State private var eventSpec = #"{"event":"document.changed"}"#
    @State private var enabled = true
    @State private var isSaving = false
    @State private var errorMessage: String?

    private var flows: [String] {
        session.pipelines.revisions[source.pipeline.pipeline]?
            .first(where: { $0.reference.version == source.pipeline.version })?
            .spec.flows.map(\.name) ?? []
    }

    var body: some View {
        VStack(alignment: .leading, spacing: .lg) {
            StyledText(
                trigger == nil ? "Add trigger" : "Edit trigger",
                Typography.setLG
            )
            Picker("Flow", selection: $flow) {
                ForEach(flows, id: \.self) { Text($0).tag($0) }
            }
            Picker("Type", selection: $kind) {
                Text("Interval").tag("interval")
                Text("Cron").tag("cron")
                Text("Event").tag("event")
            }
            if kind == "interval" {
                TextField("Seconds", text: $intervalSeconds)
            } else if kind == "cron" {
                TextField("Five-field cron expression", text: $cron)
                TextField("IANA timezone", text: $timezone)
            } else {
                Text("Event match specification (JSON)")
                    .windexStyle(Typography.label)
                TextEditor(text: $eventSpec)
                    .font(.system(.callout, design: .monospaced))
                    .frame(minHeight: 110)
                    .padding(.xs)
                    .background(theme.palette.plate)
                Text(
                    "Event trigger specifications are backend-defined and are preserved exactly."
                )
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)
            }
            Toggle("Enabled", isOn: $enabled)
            if let errorMessage {
                Text(errorMessage).foregroundStyle(theme.palette.rust)
            }
            Spacer()
            HStack {
                Button("Cancel") { dismiss() }
                Spacer()
                Button(isSaving ? "Saving…" : (trigger == nil ? "Add trigger" : "Save trigger")) {
                    Task { await save() }
                }
                .buttonStyle(.borderedProminent)
                .disabled(isSaving || flow.isEmpty)
            }
        }
        .padding(.xl)
        .background(theme.palette.ink)
        .onAppear { populate() }
    }

    private func save() async {
        isSaving = true
        defer { isSaving = false }
        do {
            let spec: [String: JSONValue]
            if kind == "interval" {
                guard let seconds = Int(intervalSeconds), seconds > 0 else {
                    throw SourceFormError.json("Interval seconds must be positive.")
                }
                spec = ["seconds": .int(seconds)]
            } else if kind == "cron" {
                spec = ["cron": .string(cron), "timezone": .string(timezone)]
            } else {
                spec = try JSONDecoder().decode(
                    [String: JSONValue].self,
                    from: Data(eventSpec.utf8)
                )
            }
            if let trigger {
                try await session.updateTrigger(
                    source: source.name,
                    id: trigger.id,
                    flow: flow,
                    type: kind,
                    spec: spec,
                    enabled: enabled
                )
            } else {
                try await session.createTrigger(
                    source: source.name,
                    flow: flow,
                    type: kind,
                    spec: spec,
                    enabled: enabled
                )
            }
            dismiss()
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func populate() {
        guard let trigger else {
            flow = flows.first ?? ""
            return
        }
        flow = trigger.flowName
        kind = trigger.triggerType
        enabled = trigger.enabled
        let spec = (try? trigger.triggerSpec.additionalProperties.decode(
            [String: JSONValue].self
        )) ?? [:]
        intervalSeconds = spec["seconds"]?.displayString ?? "3600"
        cron = spec["cron"]?.stringValue ?? "0 * * * *"
        timezone = spec["timezone"]?.stringValue ?? "UTC"
        if let data = try? JSONEncoder().encode(spec) {
            eventSpec = String(decoding: data, as: UTF8.self)
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
