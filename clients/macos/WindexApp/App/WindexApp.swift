import SwiftUI
import WindexUI

@main
struct WindexApp: App {
    @State private var model = AppModel()
    #if DEBUG
    @State private var uiTestSession = WindexUITestFixture.session()
    private let usesUITestFixture = ProcessInfo.processInfo.arguments.contains(
        "-ui-testing-source-run-gate"
    )
    #endif

    var body: some Scene {
        WindowGroup {
            #if DEBUG
            if usesUITestFixture {
                PipelinesView(appModel: model)
                    .environment(uiTestSession)
                    .windexTheme(.dark)
                    .preferredColorScheme(.dark)
            } else {
                appShell
            }
            #else
            appShell
            #endif
        }
        .defaultSize(width: 1200, height: 760)
        .windowResizability(.contentMinSize)
    }

    private var appShell: some View {
        AppShellView(model: model)
            .windexTheme(.dark)
            .preferredColorScheme(.dark)
            .task {
                await model.restore()
            }
    }
}
