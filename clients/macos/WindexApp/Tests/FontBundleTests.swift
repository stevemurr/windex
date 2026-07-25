import Foundation
import Testing
@testable import Windex

@Suite("Bundled fonts")
struct FontBundleTests {
    @Test("the designed faces are registered from the app bundle")
    func fontsAreAvailable() {
        #expect(AppFontRegistry.missingFonts.isEmpty)
    }

    @Test("the app explains why it needs local-network access")
    func localNetworkUsageIsDeclared() {
        let explanation = Bundle.main.object(
            forInfoDictionaryKey: "NSLocalNetworkUsageDescription"
        ) as? String

        #expect(explanation == "Connect to your windex server on this local network.")
    }
}
