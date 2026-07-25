import SwiftUI
import WindexUI

struct PairingView: View {
    @Bindable var model: AppModel
    @Environment(\.windexTheme) private var theme
    @State private var candidateToken = ""
    @FocusState private var focusedField: Field?

    private enum Field {
        case address
        case token
    }

    var body: some View {
        ZStack {
            theme.palette.ink.ignoresSafeArea()
            VStack(alignment: .leading, spacing: .xl) {
                header
                Hairline()
                connectionForm
                if let failure {
                    failureMessage(failure)
                }
            }
            .padding(.xl)
            .frame(maxWidth: 560)
            .background(theme.palette.plate)
            .overlay {
                Rectangle()
                    .stroke(theme.palette.rule, lineWidth: Layout.hairline)
            }
        }
        .foregroundStyle(theme.palette.paper)
        .onSubmit { submit() }
        .onChange(of: model.connectionState) { _, state in
            if case .tokenRequired = state {
                focusedField = .token
            } else if case .ready = state {
                candidateToken = ""
            }
        }
        .task {
            if model.backendAddress.isEmpty {
                focusedField = .address
            }
        }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: .sm) {
            StyledText("Windex", Typography.masthead)
                .foregroundStyle(theme.palette.cyan)
            StyledText("Pair this Mac", Typography.setLG)
            Text("Connect to the windex control plane on your network.")
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.graphite)
        }
    }

    private var connectionForm: some View {
        VStack(alignment: .leading, spacing: .md) {
            fieldLabel("Backend address")
            TextField("spark.local:8100", text: $model.backendAddress)
                .textContentType(.URL)
                .disableAutocorrection(true)
                .focused($focusedField, equals: .address)
                .windexStyle(Typography.data)
                .textFieldStyle(WindexFieldStyle())
                .accessibilityLabel("Backend address")

            if needsToken {
                fieldLabel("Admin token")
                SecureField("Token", text: $candidateToken)
                    .focused($focusedField, equals: .token)
                    .windexStyle(Typography.data)
                    .textFieldStyle(WindexFieldStyle())
                    .accessibilityLabel("Admin token")
            }

            HStack(spacing: .sm) {
                Button(action: submit) {
                    if isConnecting {
                        ProgressView()
                            .controlSize(.small)
                            .accessibilityLabel("Connecting")
                    } else {
                        Text(needsToken ? "Pair" : "Connect")
                    }
                }
                .buttonStyle(.borderedProminent)
                .disabled(isConnecting || model.backendAddress.isEmpty)

                if canRetry {
                    Button("Retry") {
                        Task { await model.retryConnection() }
                    }
                    .buttonStyle(.bordered)
                    .disabled(isConnecting)
                }

                if model.currentProfile != nil {
                    Button("Change backend") {
                        candidateToken = ""
                        model.changeBackend()
                        focusedField = .address
                    }
                    .buttonStyle(.plain)
                    .foregroundStyle(theme.palette.graphite)
                }
            }
        }
    }

    private func fieldLabel(_ value: String) -> some View {
        StyledText(value, Typography.eyebrow)
            .foregroundStyle(theme.palette.graphite)
    }

    private func failureMessage(_ failure: ConnectionFailure) -> some View {
        VStack(alignment: .leading, spacing: .xs) {
            Text(failure.title)
                .windexStyle(Typography.label)
                .foregroundStyle(theme.palette.rust)
            Text(failure.guidance)
                .windexStyle(Typography.body)
                .foregroundStyle(
                    failure == .unauthorized
                        ? theme.palette.amber : theme.palette.graphite)
                .textSelection(.enabled)
        }
        .padding(.md)
        .frame(maxWidth: .infinity, alignment: .leading)
        .overlay(alignment: .leading) {
            Rectangle()
                .fill(failure == .unauthorized
                      ? theme.palette.amber : theme.palette.rust)
                .frame(width: Layout.hairline)
        }
    }

    private var needsToken: Bool {
        switch model.connectionState {
        case .tokenRequired:
            true
        case .failed(_, .unauthorized):
            true
        default:
            false
        }
    }

    private var isConnecting: Bool {
        if case .connecting = model.connectionState { return true }
        return false
    }

    private var failure: ConnectionFailure? {
        guard case .failed(_, let failure) = model.connectionState else {
            return nil
        }
        return failure
    }

    private var canRetry: Bool {
        guard case .failed(_, let failure) = model.connectionState else {
            return false
        }
        if case .unreachable = failure { return true }
        return false
    }

    private func submit() {
        Task {
            await model.connect(
                model.backendAddress,
                candidateToken: needsToken ? candidateToken : nil,
                useStoredToken: false)
        }
    }
}

private struct WindexFieldStyle: TextFieldStyle {
    @Environment(\.windexTheme) private var theme

    func _body(configuration: TextField<Self._Label>) -> some View {
        configuration
            .padding(.horizontal, Layout.Space.sm.points)
            .frame(height: 36)
            .background(theme.palette.ink)
            .overlay {
                RoundedRectangle(cornerRadius: Layout.Radius.control)
                    .stroke(theme.palette.rule, lineWidth: Layout.hairline)
            }
    }
}
