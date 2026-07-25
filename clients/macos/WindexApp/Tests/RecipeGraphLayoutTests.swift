import Testing
@testable import Windex

@Suite("Recipe graph layout")
struct RecipeGraphLayoutTests {
    @Test("converging paths share a destination level")
    func convergingPaths() {
        let layout = RecipeGraphLayout(
            nodes: ["events", "repos", "store"],
            edges: [["events", "store"], ["repos", "store"]])

        #expect(layout.levels["events"] == 0)
        #expect(layout.levels["repos"] == 0)
        #expect(layout.levels["store"] == 1)
        #expect(layout.rows["events"] == 0)
        #expect(layout.rows["repos"] == 1)
        #expect(layout.maximumRows == 2)
    }

    @Test("a chain preserves execution depth")
    func chain() {
        let layout = RecipeGraphLayout(
            nodes: ["seed", "get", "extract", "store"],
            edges: [
                ["seed", "get"],
                ["get", "extract"],
                ["extract", "store"],
            ])

        #expect(layout.levelCount == 4)
        #expect(layout.levels["store"] == 3)
    }
}
