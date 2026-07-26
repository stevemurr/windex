import SwiftUI
import WindexKit

/// Picks the control for a `Param`, per the table in `DESIGN.md` §5.1.
///
/// The switch is on `editor`, never on `key` — that is the whole contract. An
/// editor this client doesn't know falls through to a text field rather than
/// rendering nothing, so a newer server degrades one control instead of leaving
/// a hole in the form.
struct FieldEditor: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param
    let configuredSecretReferences: [String]

    var body: some View {
        switch param.editor {
        case .checkbox:
            BoolEditor(model: model, param: param)
        case .number:
            NumberEditor(model: model, param: param)
        case .select:
            SelectEditor(model: model, param: param)
        case .multiselect:
            MultiSelectEditor(model: model, param: param)
        case .stringList, .regexList:
            StringListEditor(model: model, param: param)
        case .secret:
            if param.kind == .secretRef,
               !configuredSecretReferences.isEmpty {
                SecretReferenceEditor(
                    model: model,
                    param: param,
                    configuredSecretReferences: configuredSecretReferences
                )
            } else {
                SecretEditor(model: model, param: param)
            }
        case .textarea, .json:
            TextAreaEditor(model: model, param: param,
                           validatesJSON: param.editor == .json)
        case .datepicker:
            DateEditor(model: model, param: param)
        case .keyValue:
            KeyValueEditor(model: model, param: param)
        case .hidden:
            EmptyView()
        case .textfield, .url, .duration, .unknown:
            TextEditorField(model: model, param: param)
        }
    }
}

// MARK: - Text

private struct TextEditorField: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param

    var body: some View {
        TextField(param.title, text: binding)
            .textFieldStyle(.plain)
            // A URL is machine text and reads as such — `data` face, per §3.2.
            .windexStyle(param.editor == .url ? Typography.data : Typography.body)
            .foregroundStyle(theme.palette.paper)
            .padding(.horizontal, .xs)
            .padding(.vertical, .xxs)
            .fieldChrome()
    }

    private var binding: Binding<String> {
        Binding(
            get: { model.value(for: param)?.displayString ?? "" },
            set: { model.set(param, $0.isEmpty ? nil : .string($0)) }
        )
    }
}

private struct TextAreaEditor: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param
    let validatesJSON: Bool

    @State private var text = ""
    @State private var jsonError: String?

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            TextEditor(text: $text)
                .scrollContentBackground(.hidden)
                .windexStyle(Typography.data)
                .foregroundStyle(theme.palette.paper)
                .frame(minHeight: 88)
                .padding(.xxs)
                .fieldChrome()
                .onChange(of: text) { _, new in commit(new) }
                .onAppear { text = model.value(for: param)?.displayString ?? "" }

            if let jsonError {
                Text(jsonError)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.rust)
            }
        }
    }

    /// JSON is parsed on every keystroke so the error appears where it was
    /// typed, but the model is only updated when it parses — a half-typed object
    /// must not clear the field's value.
    private func commit(_ new: String) {
        guard validatesJSON else {
            model.set(param, new.isEmpty ? nil : .string(new))
            return
        }
        if new.isEmpty {
            jsonError = nil
            model.set(param, nil)
            return
        }
        do {
            let value = try JSONDecoder().decode(JSONValue.self, from: Data(new.utf8))
            jsonError = nil
            model.set(param, value)
        } catch {
            jsonError = "Not valid JSON"
        }
    }
}

// MARK: - Scalars

private struct BoolEditor: View {
    @Bindable var model: FormModel
    let param: Param

    var body: some View {
        Toggle(param.title, isOn: Binding(
            get: { model.value(for: param)?.boolValue ?? false },
            set: { model.set(param, .bool($0)) }
        ))
        .toggleStyle(.switch)
        .labelsHidden()
    }
}

private struct NumberEditor: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param

    var body: some View {
        HStack(spacing: .xs) {
            TextField(param.title, text: binding)
                .textFieldStyle(.plain)
                .windexStyle(Typography.data)      // tabular, per §3.2
                .foregroundStyle(theme.palette.paper)
                .multilineTextAlignment(.trailing)
                .frame(maxWidth: 120)
                .padding(.horizontal, .xs)
                .padding(.vertical, .xxs)
                .fieldChrome()

            if let unit = param.unit {
                Text(unit)
                    .windexStyle(Typography.body)
                    .foregroundStyle(theme.palette.graphite)
            }

            Stepper(param.title, onIncrement: { step(+1) }, onDecrement: { step(-1) })
                .labelsHidden()

            if let range = rangeCaption {
                Text(range)
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
            Spacer(minLength: 0)
        }
    }

    private var binding: Binding<String> {
        Binding(
            get: { model.value(for: param)?.displayString ?? "" },
            set: { text in
                guard !text.isEmpty else { model.set(param, nil); return }
                guard let number = Double(text) else { return }   // reject junk keystrokes
                model.set(param, param.kind == .int ? .int(Int(number)) : .double(number))
            }
        )
    }

    /// The bounds are the operator-resolved ones the server will actually
    /// enforce, so showing them is honest rather than aspirational.
    private var rangeCaption: String? {
        switch (param.lo, param.hi) {
        case let (lo?, hi?): return "\(format(lo))–\(format(hi))"
        case let (lo?, nil): return "min \(format(lo))"
        case let (nil, hi?): return "max \(format(hi))"
        default: return nil
        }
    }

    private func format(_ value: Double) -> String {
        value == value.rounded() ? String(Int(value)) : String(value)
    }

    private func step(_ direction: Double) {
        let current = model.value(for: param)?.doubleValue ?? param.lo ?? 0
        // A float param with a sub-unit floor (arXiv's 3.0s interval) should not
        // step by 1 — that skips most of its usable range.
        let increment: Double = param.kind == .int ? 1 : 0.5
        let next = current + direction * increment
        let bounded = min(max(next, param.lo ?? -.infinity), param.hi ?? .infinity)
        model.set(param, param.kind == .int ? .int(Int(bounded)) : .double(bounded))
    }
}

private struct DateEditor: View {
    @Bindable var model: FormModel
    let param: Param

    var body: some View {
        DatePicker(param.title, selection: binding, displayedComponents: .date)
            .datePickerStyle(.field)
            .labelsHidden()
    }

    private var binding: Binding<Date> {
        Binding(
            get: {
                guard let raw = model.value(for: param)?.stringValue else { return .now }
                return Self.formatter.date(from: raw) ?? .now
            },
            set: { model.set(param, .string(Self.formatter.string(from: $0))) }
        )
    }

    /// `kind: "date"` is an ISO calendar date on the wire, not a timestamp.
    private static let formatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd"
        f.timeZone = TimeZone(secondsFromGMT: 0)
        return f
    }()
}

// MARK: - Choice

private struct SelectEditor: View {
    @Bindable var model: FormModel
    let param: Param

    var body: some View {
        Picker(param.title, selection: binding) {
            ForEach(param.choices, id: \.self) { choice in
                Text(param.title(forChoice: choice)).tag(choice)
            }
        }
        .labelsHidden()
        .pickerStyle(.menu)
        .fixedSize()
    }

    private var binding: Binding<String> {
        Binding(
            get: { model.value(for: param)?.stringValue ?? param.choices.first ?? "" },
            set: { model.set(param, .string($0)) }
        )
    }
}

private struct MultiSelectEditor: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param

    var body: some View {
        Menu {
            ForEach(param.choices, id: \.self) { choice in
                Toggle(param.title(forChoice: choice), isOn: binding(for: choice))
            }
        } label: {
            Text(summary)
                .windexStyle(Typography.body)
                .foregroundStyle(selected.isEmpty
                                 ? theme.palette.graphite : theme.palette.paper)
        }
        .menuStyle(.borderlessButton)
        .fixedSize()
    }

    private var selected: [String] {
        model.value(for: param)?.stringArrayValue ?? []
    }

    private var summary: String {
        switch selected.count {
        case 0: return "None"
        case 1: return param.title(forChoice: selected[0])
        default: return "\(selected.count) selected"
        }
    }

    private func binding(for choice: String) -> Binding<Bool> {
        Binding(
            get: { selected.contains(choice) },
            set: { isOn in
                var next = selected
                if isOn {
                    if !next.contains(choice) { next.append(choice) }
                } else {
                    next.removeAll { $0 == choice }
                }
                model.set(param, .array(next.map(JSONValue.string)))
            }
        )
    }
}

// MARK: - Lists

/// Backs `stringList`, `regexList` and `csv`.
///
/// `csv` is the subtle one: it is a **string** on the wire — the raw
/// comma-separated form the server's `*_list()` helpers parse — and rendering it
/// as a list is a client-side affordance only. Sending an array would be a
/// different value, so the join happens on write.
private struct StringListEditor: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param

    private var items: [String] {
        guard let value = model.value(for: param) else { return [] }
        if param.kind == .csv {
            return value.stringValue?
                .split(separator: ",")
                .map { $0.trimmingCharacters(in: .whitespaces) }
                .filter { !$0.isEmpty } ?? []
        }
        return value.stringArrayValue ?? []
    }

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            ForEach(Array(items.enumerated()), id: \.offset) { index, item in
                HStack(spacing: .xs) {
                    TextField("", text: rowBinding(index))
                        .textFieldStyle(.plain)
                        .windexStyle(Typography.data)
                        .foregroundStyle(theme.palette.paper)
                        .padding(.horizontal, .xs)
                        .padding(.vertical, .xxs)
                        .fieldChrome()

                    if let problem = problem(with: item) {
                        Text(problem)
                            .windexStyle(Typography.dataSM)
                            .foregroundStyle(theme.palette.rust)
                    }

                    Button {
                        remove(index)
                    } label: {
                        Image(systemName: "minus")
                    }
                    .buttonStyle(.borderless)
                    .foregroundStyle(theme.palette.graphite)
                    .accessibilityLabel("Remove \(item)")
                }
            }

            Button("Add") { append() }
                .buttonStyle(.borderless)
                .windexStyle(Typography.label)
                .foregroundStyle(theme.palette.cyan)
                // maxItems is enforced server-side; disabling here explains the
                // cap instead of letting someone hit a 422 on submit.
                .disabled(param.maxItems.map { items.count >= $0 } ?? false)
        }
    }

    /// Per-keystroke regex validation, per §5.1. A bad pattern is caught here
    /// rather than hours later inside a worker.
    private func problem(with item: String) -> String? {
        guard param.kind == .regexList, !item.isEmpty else { return nil }
        return (try? NSRegularExpression(pattern: item)) == nil ? "invalid regex" : nil
    }

    private func rowBinding(_ index: Int) -> Binding<String> {
        Binding(
            get: { index < items.count ? items[index] : "" },
            set: { new in
                var next = items
                guard index < next.count else { return }
                next[index] = new
                write(next)
            }
        )
    }

    private func append() {
        write(items + [""])
    }

    private func remove(_ index: Int) {
        var next = items
        guard index < next.count else { return }
        next.remove(at: index)
        write(next)
    }

    private func write(_ next: [String]) {
        if param.kind == .csv {
            model.set(param, .string(next.joined(separator: ",")))
        } else {
            model.set(param, .array(next.map(JSONValue.string)))
        }
    }
}

private struct KeyValueEditor: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param

    private var pairs: [(key: String, value: String)] {
        (model.value(for: param)?.objectValue ?? [:])
            .map { ($0.key, $0.value.displayString) }
            .sorted { $0.0 < $1.0 }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            ForEach(pairs, id: \.key) { pair in
                HStack(spacing: .xs) {
                    Text(pair.key)
                        .windexStyle(Typography.data)
                        .foregroundStyle(theme.palette.graphite)
                        .frame(width: 140, alignment: .leading)

                    TextField("", text: valueBinding(pair.key))
                        .textFieldStyle(.plain)
                        .windexStyle(Typography.data)
                        .foregroundStyle(theme.palette.paper)
                        .padding(.horizontal, .xs)
                        .padding(.vertical, .xxs)
                        .fieldChrome()

                    Button {
                        write(pairs.filter { $0.key != pair.key })
                    } label: {
                        Image(systemName: "minus")
                    }
                    .buttonStyle(.borderless)
                    .foregroundStyle(theme.palette.graphite)
                    .accessibilityLabel("Remove \(pair.key)")
                }
            }

            Button("Add") {
                write(pairs + [(uniqueKey(), "")])
            }
            .buttonStyle(.borderless)
            .windexStyle(Typography.label)
            .foregroundStyle(theme.palette.cyan)
        }
    }

    private func uniqueKey() -> String {
        var name = "key"
        var n = 1
        let existing = Set(pairs.map(\.key))
        while existing.contains(name) {
            n += 1
            name = "key\(n)"
        }
        return name
    }

    private func valueBinding(_ key: String) -> Binding<String> {
        Binding(
            get: { pairs.first { $0.key == key }?.value ?? "" },
            set: { new in
                write(pairs.map { $0.key == key ? ($0.key, new) : $0 })
            }
        )
    }

    private func write(_ next: [(key: String, value: String)]) {
        model.set(param, .object(Dictionary(
            next.map { ($0.key, JSONValue.string($0.value)) },
            uniquingKeysWith: { _, last in last })))
    }
}

// MARK: - Secret

/// A `secret_ref` is the name of an operator-configured secret, never secret
/// material. Source creation receives the configured names and exposes them
/// explicitly instead of asking the operator to type a masked identifier.
private struct SecretReferenceEditor: View {
    @Bindable var model: FormModel
    let param: Param
    let configuredSecretReferences: [String]

    private var choices: [String] {
        let configured = Set(configuredSecretReferences)
        let allowed = param.allow.isEmpty
            ? configured
            : configured.intersection(param.allow)
        var values = allowed.sorted()
        if let current = model.value(for: param)?.stringValue,
           !values.contains(current) {
            values.append(current)
        }
        return values
    }

    var body: some View {
        Picker(
            param.title,
            selection: Binding(
                get: { model.value(for: param)?.stringValue },
                set: { model.set(param, $0.map(JSONValue.string)) }
            )
        ) {
            Text("Choose a configured secret").tag(String?.none)
            ForEach(choices, id: \.self) { name in
                Text(name).tag(Optional(name))
            }
        }
        .labelsHidden()
    }
}

/// Write-only. The server never echoes a secret's value, so the control must
/// show that one is *set* without pretending to know it — an empty required
/// field would read as "unconfigured" when it isn't.
private struct SecretEditor: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param

    @State private var entry = ""

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            SecureField(placeholder, text: $entry)
                .textFieldStyle(.plain)
                .windexStyle(Typography.data)
                .foregroundStyle(theme.palette.paper)
                .padding(.horizontal, .xs)
                .padding(.vertical, .xxs)
                .fieldChrome()
                .onChange(of: entry) { _, new in
                    model.set(param, new.isEmpty ? nil : .string(new))
                }

            if !param.allow.isEmpty {
                // A secret_ref names an operator-provisioned key from an
                // allowlist; it never carries the credential itself.
                Text("One of: \(param.allow.joined(separator: ", "))")
                    .windexStyle(Typography.dataSM)
                    .foregroundStyle(theme.palette.graphite)
            }
        }
    }

    private var placeholder: String {
        model.value(for: param) != nil ? "Set — type to replace" : "Not set"
    }
}

// MARK: - Chrome

extension View {
    /// The shared input treatment: `plate` ground, `rule` hairline, radius 4.
    func fieldChrome() -> some View {
        modifier(FieldChrome())
    }
}

private struct FieldChrome: ViewModifier {
    @Environment(\.windexTheme) private var theme

    func body(content: Content) -> some View {
        content
            .background(
                RoundedRectangle(cornerRadius: Layout.Radius.control)
                    .fill(theme.palette.plate)
            )
            .overlay(
                RoundedRectangle(cornerRadius: Layout.Radius.control)
                    .strokeBorder(theme.palette.rule, lineWidth: Layout.hairline)
            )
    }
}
