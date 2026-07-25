// swift-tools-version: 6.0
import PackageDescription

// One dependency: swift-openapi-runtime, which the generated admin DTOs in
// Sources/WindexKit/Generated need at compile time.
//
// The GENERATOR is deliberately not here — it lives in ../../Tools, because it
// pulls a seven-package graph that would otherwise be resolved on every clean
// checkout just to compile. Generated code is checked in and refreshed by
// `Tools/generate.sh` when the control plane changes, so building this package
// still needs no Python environment and no code generation step.
//
// The transport, the SSE client and everything touching /v1/search stay
// hand-written; the mock HTTP server the tests run against is built on
// Network.framework (in the SDK) rather than adding a server dependency.
let package = Package(
    name: "WindexKit",
    platforms: [.macOS(.v14)],
    products: [
        .library(name: "WindexKit", targets: ["WindexKit"]),
        .library(name: "WindexUI", targets: ["WindexUI"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-openapi-runtime",
                 from: "1.8.0"),
    ],
    targets: [
        .target(
            name: "WindexKit",
            dependencies: [
                .product(name: "OpenAPIRuntime", package: "swift-openapi-runtime"),
            ],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        // The design system and the generic form renderer. Separate from
        // WindexKit so the transport stays testable headlessly and importable
        // from anything that doesn't draw — a CLI, a share extension, a test
        // harness.
        .target(
            name: "WindexUI",
            dependencies: ["WindexKit"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "WindexUITests",
            dependencies: ["WindexUI", "WindexKit"],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
        .testTarget(
            name: "WindexKitTests",
            dependencies: ["WindexKit"],
            // Fixtures are generated from windex's own schema by
            // Fixtures/generate_fixtures.py and checked in, so the tests decode
            // what the server actually emits without needing it running.
            resources: [.copy("Fixtures")],
            swiftSettings: [.swiftLanguageMode(.v6)]
        ),
    ]
)
