// swift-tools-version: 6.0
import PackageDescription

// A holder for the code-generator executable, deliberately kept SEPARATE from
// WindexKit's manifest.
//
// swift-openapi-generator pulls a seven-package graph (OpenAPIKit, Yams,
// swift-algorithms, swift-numerics, argument-parser…). Declaring it as a
// dependency of WindexKit would make every clean checkout resolve all of it just
// to compile, and Xcode would carry the graph forever. Since the generated code
// is checked in and only regenerated when the API changes, the generator is a
// build-time tool for whoever regenerates — not a dependency of the library.
//
// WindexKit itself depends on swift-openapi-runtime alone.
//
//   ./Tools/generate.sh      regenerate WindexKit/Generated from the specs
let package = Package(
    name: "WindexTools",
    platforms: [.macOS(.v14)],
    dependencies: [
        .package(url: "https://github.com/apple/swift-openapi-generator",
                 from: "1.13.0"),
    ],
    targets: [
        // SPM requires at least one target; nothing is built from it. The
        // generator executable comes from the dependency above and is invoked
        // with `swift run --package-path Tools swift-openapi-generator`.
        .target(name: "WindexToolsPlaceholder", path: "Placeholder"),
    ]
)
