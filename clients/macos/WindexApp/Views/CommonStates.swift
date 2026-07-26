import SwiftUI
import WindexUI

struct SourceFailureView: View {
    let message: String
    let retry: () -> Void
    @Environment(\.windexTheme) private var theme

    var body: some View {
        VStack(alignment: .leading, spacing: .sm) {
            Text(message)
                .windexStyle(Typography.body)
                .foregroundStyle(theme.palette.rust)
                .textSelection(.enabled)
            Button("Retry", action: retry)
                .buttonStyle(.bordered)
        }
        .padding(.xl)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}
