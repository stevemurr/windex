import SwiftUI
import WindexKit

/// Renders a form from `Param` JSON and nothing else.
///
/// This is the most reused component in the app: it draws windex settings, job
/// argument dialogs, and — later, unchanged — recipe install params and the
/// graph node inspector. **It hardcodes no field.** Everything it knows comes
/// from the schema the server sent, which is why a windex that gains a module or
/// a setting needs no client release.
///
/// Layout per `DESIGN.md` §5.1: label above the control, not beside — labels
/// vary in length and a side-by-side grid ragged-rights badly.
public struct SchemaForm: View {
    @Environment(\.windexTheme) private var theme
    @Bindable private var model: FormModel
    private let configuredSecretReferences: [String]

    public init(
        model: FormModel,
        configuredSecretReferences: [String] = []
    ) {
        self.model = model
        self.configuredSecretReferences = configuredSecretReferences
    }

    public var body: some View {
        VStack(alignment: .leading, spacing: .lg) {
            ForEach(model.sections) { section in
                if !section.isEmpty {
                    SchemaFormSection(
                        model: model,
                        section: section,
                        configuredSecretReferences: configuredSecretReferences
                    )
                }
            }
        }
        .frame(maxWidth: Layout.proseMeasure, alignment: .leading)
    }
}

struct SchemaFormSection: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let section: FormSection
    let configuredSecretReferences: [String]

    @State private var advancedExpanded = false

    var body: some View {
        VStack(alignment: .leading, spacing: .md) {
            if let name = section.name {
                StyledText(name, Typography.eyebrow)
                    .foregroundStyle(theme.palette.graphite)
            }

            ForEach(section.fields) { param in
                SchemaField(
                    model: model,
                    param: param,
                    configuredSecretReferences: configuredSecretReferences
                )
            }

            if !section.advanced.isEmpty {
                DisclosureGroup(isExpanded: $advancedExpanded) {
                    VStack(alignment: .leading, spacing: .md) {
                        ForEach(section.advanced) { param in
                            SchemaField(
                                model: model,
                                param: param,
                                configuredSecretReferences: configuredSecretReferences
                            )
                        }
                    }
                    .padding(.top, .sm)
                } label: {
                    StyledText("Advanced", Typography.eyebrow)
                        .foregroundStyle(theme.palette.graphite)
                }
                .tint(theme.palette.cyan)
            }
        }
    }
}

/// One control: label, editor, and whichever of helper text / clamp preview /
/// error applies.
struct SchemaField: View {
    @Environment(\.windexTheme) private var theme
    @Bindable var model: FormModel
    let param: Param
    let configuredSecretReferences: [String]

    private var isEnabled: Bool { model.isEnabled(param) }
    private var error: String? { model.error(for: param) }

    var body: some View {
        VStack(alignment: .leading, spacing: .xxs) {
            HStack(spacing: .xxs) {
                Text(param.title)
                    .windexStyle(Typography.label)
                    .foregroundStyle(theme.palette.paper)
                if param.required {
                    Text("required")
                        .windexStyle(Typography.eyebrow)
                        .foregroundStyle(theme.palette.graphite)
                }
            }

            FieldEditor(
                model: model,
                param: param,
                configuredSecretReferences: configuredSecretReferences
            )
                .disabled(!isEnabled)

            footer
        }
        // A control gated by an unmet dependency is dimmed, not hidden (§5.1):
        // an operator must be able to see that the setting exists and learn what
        // it depends on.
        .opacity(isEnabled ? 1 : 0.45)
        .accessibilityElement(children: .contain)
        .accessibilityLabel(param.title)
        .accessibilityHint(model.helperText(for: param) ?? "")
    }

    @ViewBuilder
    private var footer: some View {
        if let error {
            Text(error)
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.rust)
        } else if let preview = model.clampPreview(for: param) {
            // The server will silently adjust this. Say so before it happens,
            // not after — "I typed 0.5 and got 1.0" must never be a mystery.
            Text("Will save as \(preview.displayString)\(param.unit.map { " \($0)" } ?? "")")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.amber)
        } else if let helper = model.helperText(for: param) {
            Text(helper)
                .windexStyle(Typography.body)
                .foregroundStyle(helperColour(helper))
        }
    }

    private func helperColour(_ helper: String) -> Color {
        // A clamp notice from the last save is an adjustment the operator should
        // notice; everything else here is quiet supporting text.
        model.clampNotices[param.key] == helper
            ? theme.palette.amber
            : theme.palette.graphite
    }
}
