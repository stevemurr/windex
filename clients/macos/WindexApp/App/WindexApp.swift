import SwiftUI
import WindexUI

@main
struct WindexApp: App {
    @State private var model = AppModel()

    var body: some Scene {
        WindowGroup {
            AppShellView(model: model)
                .windexTheme(.dark)
                .preferredColorScheme(.dark)
                .task {
                    await model.restore()
                }
        }
        .defaultSize(width: 1200, height: 760)
        .windowResizability(.contentMinSize)
    }
}
