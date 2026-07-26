import Testing
@testable import WindexKit

@Suite("Memory conversation ingestion")
struct MemoryIngestTests {
    private let conversation = "0f9d2a41-3c7e-4b18-9a05-6d1f8c2e4b77"

    @Test("a full replacement derives partition-safe document identity")
    func replacement() throws {
        let batch = try MemoryIngestBatch.replacement(
            conversationID: conversation,
            chunks: [
                try MemoryConversationChunk(
                    chunkIndex: 0,
                    messageRangeStart: 0,
                    messageRangeEnd: 4,
                    text: "First chunk",
                    title: "Design review"
                ),
                try MemoryConversationChunk(
                    chunkIndex: 7,
                    messageRangeStart: 5,
                    messageRangeEnd: 9,
                    text: "Second chunk"
                ),
            ]
        )

        #expect(batch.mode == "full")
        #expect(batch.partition == conversation)
        #expect(batch.documents.map(\.id) == [
            "\(conversation)/00000",
            "\(conversation)/00007",
        ])
        #expect(batch.documents.allSatisfy {
            $0.id.hasPrefix("\(conversation)/")
        })
        #expect(batch.documents[0].fields == [
            "conversation_id": .string(conversation),
            "chunk_index": .int(0),
            "message_range": .array([.int(0), .int(4)]),
        ])
        #expect(batch.documents[1].fields == [
            "conversation_id": .string(conversation),
            "chunk_index": .int(7),
            "message_range": .array([.int(5), .int(9)]),
        ])
    }

    @Test("deletion is an empty full replacement of one partition")
    func deletion() throws {
        let batch = try MemoryIngestBatch.deletion(
            conversationID: "  \(conversation)  "
        )

        #expect(batch.mode == "full")
        #expect(batch.partition == conversation)
        #expect(batch.documents.isEmpty)
    }

    @Test("unattributed and ambiguous memory batches cannot be constructed")
    func invalidBatches() throws {
        #expect(throws: MemoryIngestError.missingConversationID) {
            _ = try MemoryIngestBatch.deletion(conversationID: "  ")
        }
        #expect(throws: MemoryIngestError.emptyReplacement) {
            _ = try MemoryIngestBatch.replacement(
                conversationID: conversation,
                chunks: []
            )
        }
        let duplicate = try MemoryConversationChunk(
            chunkIndex: 2,
            messageRangeStart: 0,
            messageRangeEnd: 1,
            text: "chunk"
        )
        #expect(throws: MemoryIngestError.duplicateChunkIndex(2)) {
            _ = try MemoryIngestBatch.replacement(
                conversationID: conversation,
                chunks: [duplicate, duplicate]
            )
        }
    }

    @Test("chunk metadata rejects malformed ranges and indexes")
    func invalidChunks() {
        #expect(throws: MemoryIngestError.negativeChunkIndex(-1)) {
            _ = try MemoryConversationChunk(
                chunkIndex: -1,
                messageRangeStart: 0,
                messageRangeEnd: 1,
                text: "chunk"
            )
        }
        #expect(
            throws: MemoryIngestError.invalidMessageRange(start: 8, end: 3)
        ) {
            _ = try MemoryConversationChunk(
                chunkIndex: 0,
                messageRangeStart: 8,
                messageRangeEnd: 3,
                text: "chunk"
            )
        }
    }
}
