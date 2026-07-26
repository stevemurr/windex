import Observation
import SwiftUI
import WindexKit
import WindexUI

@MainActor
@Observable
final class SearchModel {
    typealias SearchOperation =
        @Sendable (SearchQuery) async throws -> SearchResponse
    typealias DocumentOperation =
        @Sendable (String) async throws -> WindexKit.Document

    var query = "" {
        didSet {
            guard query != oldValue else { return }
            searchInputChanged()
        }
    }
    var source: SearchSource = .all {
        didSet {
            guard source != oldValue else { return }
            searchInputChanged()
        }
    }
    var mode: SearchMode = .hybrid {
        didSet {
            guard mode != oldValue else { return }
            searchInputChanged()
        }
    }
    var limit = 20 {
        didSet {
            guard limit != oldValue else { return }
            searchInputChanged()
        }
    }
    private(set) var sources: [SearchSource] = [.all]
    private(set) var response: SearchResponse?
    private(set) var document: WindexKit.Document?
    private(set) var isSearching = false
    private(set) var isLoadingDocument = false
    private(set) var searchErrorMessage: String?
    private(set) var documentErrorMessage: String?
    var selectedID: String?

    @ObservationIgnored private var searchGeneration: UInt64 = 0
    @ObservationIgnored private var documentGeneration: UInt64 = 0
    @ObservationIgnored private var searchTask: Task<Void, Never>?
    @ObservationIgnored private var documentTask: Task<Void, Never>?

    func useSources(_ deployments: [SourceDeployment]) {
        guard !deployments.isEmpty else { return }
        var names: [String] = []
        if deployments.contains(where: \.search.includeInAll) {
            names.append(SearchSource.all.rawValue)
        }
        names.append(contentsOf: deployments.map(\.search.searchName))
        sources = Array(Set(names)).sorted().map { SearchSource($0) }
        if !sources.contains(source), let first = sources.first {
            source = first
        }
    }

    /// Start a search owned by the current input snapshot.
    ///
    /// Cancellation reduces wasted transport work, while `searchGeneration`
    /// remains the correctness boundary: a transport may finish after
    /// cancellation and its stale completion will still be ignored.
    @discardableResult
    func submit(client: WindexClient) -> Task<Void, Never>? {
        submit { query in
            try await client.search(query)
        }
    }

    @discardableResult
    func submit(
        using operation: @escaping SearchOperation
    ) -> Task<Void, Never>? {
        let trimmed = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            invalidateSearch(clearPresentedState: true)
            return nil
        }

        invalidateSearch(clearPresentedState: true)
        let generation = searchGeneration
        let request = SearchQuery(
            q: trimmed,
            source: source,
            limit: limit,
            mode: mode
        )
        isSearching = true
        let task = Task { [weak self] in
            do {
                try Task.checkCancellation()
                let response = try await operation(request)
                guard let self else { return }
                self.completeSearch(response, generation: generation)
            } catch {
                guard let self else { return }
                self.failSearch(error, generation: generation)
            }
        }
        searchTask = task
        return task
    }

    @discardableResult
    func select(
        _ id: String?,
        client: WindexClient
    ) -> Task<Void, Never>? {
        select(id) { id in
            try await client.document(id: id)
        }
    }

    @discardableResult
    func select(
        _ id: String?,
        using operation: @escaping DocumentOperation
    ) -> Task<Void, Never>? {
        invalidateDocument(clearSelection: true)
        selectedID = id
        guard let id else { return nil }

        let generation = documentGeneration
        isLoadingDocument = true
        let task = Task { [weak self] in
            do {
                try Task.checkCancellation()
                let loaded = try await operation(id)
                guard let self else { return }
                self.completeDocument(
                    loaded,
                    id: id,
                    generation: generation
                )
            } catch {
                guard let self else { return }
                self.failDocument(
                    error,
                    id: id,
                    generation: generation
                )
            }
        }
        documentTask = task
        return task
    }

    /// Stop work when the Search workspace goes away. Any cancellation-
    /// insensitive completion is made stale before its task is cancelled.
    func cancelPending() {
        searchGeneration &+= 1
        searchTask?.cancel()
        searchTask = nil
        isSearching = false
        invalidateDocument(clearSelection: true)
    }

    private func searchInputChanged() {
        invalidateSearch(clearPresentedState: true)
    }

    private func invalidateSearch(clearPresentedState: Bool) {
        searchGeneration &+= 1
        searchTask?.cancel()
        searchTask = nil
        isSearching = false
        invalidateDocument(clearSelection: true)
        if clearPresentedState {
            response = nil
            searchErrorMessage = nil
        }
    }

    private func invalidateDocument(clearSelection: Bool) {
        documentGeneration &+= 1
        documentTask?.cancel()
        documentTask = nil
        isLoadingDocument = false
        if clearSelection {
            selectedID = nil
            document = nil
            documentErrorMessage = nil
        }
    }

    private func completeSearch(
        _ completed: SearchResponse,
        generation: UInt64
    ) {
        guard generation == searchGeneration else { return }
        searchTask = nil
        response = completed
        searchErrorMessage = nil
        isSearching = false
    }

    private func failSearch(_ error: any Error, generation: UInt64) {
        guard generation == searchGeneration else { return }
        searchTask = nil
        response = nil
        isSearching = false
        guard !(error is CancellationError) else { return }
        searchErrorMessage = (error as? WindexError)?.localizedDescription
            ?? "Search could not be completed."
    }

    private func completeDocument(
        _ completed: WindexKit.Document,
        id: String,
        generation: UInt64
    ) {
        guard generation == documentGeneration, selectedID == id else {
            return
        }
        documentTask = nil
        document = completed
        documentErrorMessage = nil
        isLoadingDocument = false
    }

    private func failDocument(
        _ error: any Error,
        id: String,
        generation: UInt64
    ) {
        guard generation == documentGeneration, selectedID == id else {
            return
        }
        documentTask = nil
        document = nil
        isLoadingDocument = false
        guard !(error is CancellationError) else { return }
        documentErrorMessage = (error as? WindexError)?.localizedDescription
            ?? "The document could not be loaded."
    }
}

struct SearchView: View {
    @Bindable var appModel: AppModel
    let client: WindexClient
    let backend: ConnectedBackend

    @State private var model = SearchModel()
    @Environment(BackendSession.self) private var session
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(spacing: 0) {
            searchBar
            Hairline()
            if model.response == nil, !model.isSearching {
                initialState
            } else {
                GeometryReader { geometry in
                    if geometry.size.width >= 800 {
                        HSplitView {
                            results
                                .frame(minWidth: 320, idealWidth: 420)
                            document
                                .frame(minWidth: 420)
                        }
                    } else if model.selectedID == nil {
                        results
                    } else {
                        document
                            .toolbar {
                                ToolbarItem(placement: .navigation) {
                                    Button {
                                        model.select(nil, client: client)
                                    } label: {
                                        Label("All results", systemImage: "chevron.left")
                                    }
                                }
                            }
                    }
                }
            }
        }
        .background(theme.palette.ink)
        .task(id: session.sources.sources) {
            model.useSources(session.sources.sources)
        }
        .onDisappear {
            model.cancelPending()
        }
    }

    private var searchBar: some View {
        HStack(spacing: .sm) {
            TextField("Search the corpus", text: $model.query)
                .textFieldStyle(.roundedBorder)
                .onSubmit { search() }
                .accessibilityLabel("Search query")

            Picker("Source", selection: $model.source) {
                ForEach(model.sources, id: \.rawValue) { source in
                    Text(source.rawValue == "all" ? "All sources" : source.rawValue)
                        .tag(source)
                }
            }
            .frame(width: 150)

            Picker("Mode", selection: $model.mode) {
                ForEach(SearchMode.allCases, id: \.self) { mode in
                    Text(mode.rawValue).tag(mode)
                }
            }
            .frame(width: 120)

            Stepper("\(model.limit)", value: $model.limit, in: 1...50)
                .frame(width: 90)
                .accessibilityLabel("Result limit")

            Button("Search", action: search)
                .buttonStyle(.borderedProminent)
                .disabled(
                    model.query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        }
        .padding(.md)
    }

    @ViewBuilder
    private var initialState: some View {
        if let error = model.searchErrorMessage {
            SourceFailureView(message: error, retry: search)
        } else {
            VStack(alignment: .leading, spacing: .sm) {
                StyledText("Search", Typography.setLG)
                Text("Search every indexed source, then inspect the stored document without leaving the app.")
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
                    .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
            }
            .padding(.xl)
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }

    private var results: some View {
        VStack(spacing: 0) {
            resultHeader
            Hairline()
            if model.isSearching {
                ProgressView()
                    .controlSize(.small)
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if let response = model.response, response.results.isEmpty {
                VStack(alignment: .leading, spacing: .sm) {
                    Text("No results.")
                        .windexStyle(Typography.label)
                    Text("The backend returned an empty index for this query. Indexing and vector availability are server state, not a client error.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                }
                .padding(.lg)
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            } else {
                List(model.response?.results ?? [], selection: selectedID) { hit in
                    SearchHitRow(hit: hit)
                        .tag(hit.id)
                }
                .listStyle(.plain)
                .scrollContentBackground(.hidden)
            }
        }
        .background(theme.palette.plate)
    }

    private var resultHeader: some View {
        HStack {
            if let response = model.response {
                Text("\(response.results.count) results · \(response.tookMs) ms")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
                if response.isDegraded {
                    StatusBadge(.attention, word: "degraded")
                }
            }
            Spacer()
        }
        .padding(.md)
    }

    private var selectedID: Binding<String?> {
        Binding(
            get: { model.selectedID },
            set: { id in
                guard id != model.selectedID else { return }
                model.select(id, client: client)
            })
    }

    @ViewBuilder
    private var document: some View {
        if model.isLoadingDocument {
            ProgressView()
                .controlSize(.small)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else if let document = model.document {
            DocumentView(document: document)
        } else if let error = model.documentErrorMessage,
                  model.selectedID != nil {
            SourceFailureView(message: error) {
                model.select(model.selectedID, client: client)
            }
        } else {
            Text("Choose a result to inspect its stored document.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }

    private func search() {
        model.submit(client: client)
    }
}

private struct SearchHitRow: View {
    let hit: SearchHit
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .xs) {
            Text(hit.title ?? hit.id)
                .windexStyle(Typography.label)
                .lineLimit(2)
            if let snippet = hit.snippet, !snippet.isEmpty {
                Text(snippet)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
                    .lineLimit(3)
            }
            HStack(spacing: .xs) {
                Text(hit.source ?? "unknown")
                Text("· \(hit.score.formatted(.number.precision(.fractionLength(3))))")
                if let stars = hit.stars { Text("· \(stars.formatted()) stars") }
                if let points = hit.points { Text("· \(points.formatted()) points") }
            }
            .windexStyle(Typography.dataSM)
            .foregroundStyle(theme.palette.graphite)
        }
        .padding(.vertical, .xs)
        .accessibilityElement(children: .combine)
    }
}

private struct DocumentView: View {
    let document: WindexKit.Document
    @Environment(\.windexTheme) private var theme

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: .md) {
                StyledText(document.title ?? document.id, Typography.setLG)
                HStack(spacing: .sm) {
                    Text(document.source ?? "unknown source")
                    Text(document.id)
                }
                .windexStyle(Typography.dataSM)
                .foregroundStyle(theme.palette.graphite)

                if let raw = document.url, let url = URL(string: raw) {
                    Link(raw, destination: url)
                        .windexStyle(Typography.dataSM)
                        .foregroundStyle(theme.palette.cyan)
                }

                Hairline()

                if let text = document.text, !text.isEmpty {
                    Text(text)
                        .windexStyle(Typography.body)
                        .textSelection(.enabled)
                        .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
                } else {
                    Text("This document has no stored text.")
                        .windexStyle(Typography.body)
                        .foregroundStyle(theme.palette.graphite)
                }

                metadata
            }
            .padding(.xl)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .background(theme.palette.ink)
    }

    private var metadata: some View {
        let hidden = Set(["id", "doc_id", "title", "url", "text", "source"])
        let rows = document.fields
            .filter { !hidden.contains($0.key) && !$0.value.displayString.isEmpty }
            .sorted { $0.key < $1.key }
        return VStack(alignment: .leading, spacing: .xs) {
            if !rows.isEmpty {
                Hairline()
                StyledText("Metadata", Typography.eyebrow)
                    .foregroundStyle(theme.palette.graphite)
                ForEach(rows, id: \.key) { key, value in
                    HStack(alignment: .firstTextBaseline) {
                        Text(key)
                            .foregroundStyle(theme.palette.graphite)
                            .frame(width: 130, alignment: .leading)
                        Text(value.displayString)
                            .textSelection(.enabled)
                    }
                    .windexStyle(Typography.dataSM)
                }
            }
        }
    }
}
