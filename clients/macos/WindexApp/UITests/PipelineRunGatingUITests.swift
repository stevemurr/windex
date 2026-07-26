import XCTest

final class PipelineRunGatingUITests: XCTestCase {
    override func setUpWithError() throws {
        continueAfterFailure = false
    }

    func testSourceOnlyFlowDisablesGenericRunAndDirectsToSource() {
        let app = XCUIApplication()
        app.launchArguments = ["-ui-testing-source-run-gate"]
        app.launch()

        let pipeline = app.staticTexts["arXiv papers"].firstMatch
        XCTAssertTrue(
            pipeline.waitForExistence(timeout: 10),
            "The arXiv Pipeline fixture did not appear."
        )
        pipeline.click()

        let run = app.buttons["Run"].firstMatch
        XCTAssertTrue(
            run.waitForExistence(timeout: 5),
            "The generic Run control did not appear."
        )
        XCTAssertFalse(
            run.isEnabled,
            "A Flow with Source-only Modules must not queue a generic Run."
        )
        XCTAssertTrue(
            app.descendants(matching: .any)["Source required"]
                .waitForExistence(timeout: 5),
            "The UI did not explain why generic Run is unavailable."
        )
        XCTAssertTrue(
            app.buttons["Open Source"].firstMatch.exists,
            "The UI did not direct the operator to the existing Source."
        )
    }
}
