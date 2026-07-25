import Testing
@testable import Windex

@Suite("Bundled fonts")
struct FontBundleTests {
    @Test("the designed faces are registered from the app bundle")
    func fontsAreAvailable() {
        #expect(AppFontRegistry.missingFonts.isEmpty)
    }
}
