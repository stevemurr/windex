import Foundation

/// One searchable chunk in a complete memory-conversation replacement.
///
/// The batch owns document identity. Callers provide semantic chunk metadata,
/// then `MemoryIngestBatch` derives the required `<conversation_id>/...` ID,
/// URL, and fields so a memory push cannot be filed outside its partition.
public struct MemoryConversationChunk: Hashable, Sendable {
    public let chunkIndex: Int
    public let messageRangeStart: Int
    public let messageRangeEnd: Int
    public let text: String
    public let title: String
    public let publishedAt: String?
    public let lang: String?

    public init(
        chunkIndex: Int,
        messageRangeStart: Int,
        messageRangeEnd: Int,
        text: String,
        title: String = "",
        publishedAt: String? = nil,
        lang: String? = nil
    ) throws {
        guard chunkIndex >= 0 else {
            throw MemoryIngestError.negativeChunkIndex(chunkIndex)
        }
        guard messageRangeStart >= 0, messageRangeEnd >= messageRangeStart else {
            throw MemoryIngestError.invalidMessageRange(
                start: messageRangeStart,
                end: messageRangeEnd
            )
        }
        self.chunkIndex = chunkIndex
        self.messageRangeStart = messageRangeStart
        self.messageRangeEnd = messageRangeEnd
        self.text = text
        self.title = title
        self.publishedAt = publishedAt
        self.lang = lang
    }

    fileprivate func document(conversationID: String) -> IngestDocument {
        let suffix = String(format: "%05d", chunkIndex)
        let encodedConversation = conversationID.addingPercentEncoding(
            withAllowedCharacters: .alphanumerics.union(
                CharacterSet(charactersIn: "-._~")
            )
        ) ?? conversationID
        return IngestDocument(
            id: "\(conversationID)/\(suffix)",
            url: "llmchat://chat/\(encodedConversation)?chunk=\(chunkIndex)",
            text: text,
            title: title,
            publishedAt: publishedAt,
            lang: lang,
            fields: [
                "conversation_id": .string(conversationID),
                "chunk_index": .int(chunkIndex),
                "message_range": .array([
                    .int(messageRangeStart),
                    .int(messageRangeEnd),
                ]),
            ]
        )
    }
}

/// A single-conversation memory push.
///
/// Memory is partition-replacing: every normal submission is a complete
/// `mode=full` snapshot of one conversation, while deletion is the same request
/// with an empty document list. Keeping those two constructors distinct avoids
/// turning an accidentally empty replacement into a silent deletion.
public struct MemoryIngestBatch: Hashable, Sendable {
    public let mode = "full"
    public let partition: String
    public let documents: [IngestDocument]

    public static func replacement(
        conversationID: String,
        chunks: [MemoryConversationChunk]
    ) throws -> MemoryIngestBatch {
        guard !chunks.isEmpty else {
            throw MemoryIngestError.emptyReplacement
        }
        let partition = try validatedConversationID(conversationID)
        var indexes = Set<Int>()
        for chunk in chunks {
            guard indexes.insert(chunk.chunkIndex).inserted else {
                throw MemoryIngestError.duplicateChunkIndex(chunk.chunkIndex)
            }
        }
        return MemoryIngestBatch(
            partition: partition,
            documents: chunks.map { $0.document(conversationID: partition) }
        )
    }

    public static func deletion(
        conversationID: String
    ) throws -> MemoryIngestBatch {
        MemoryIngestBatch(
            partition: try validatedConversationID(conversationID),
            documents: []
        )
    }

    private init(partition: String, documents: [IngestDocument]) {
        self.partition = partition
        self.documents = documents
    }

    private static func validatedConversationID(_ raw: String) throws -> String {
        let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !value.isEmpty else {
            throw MemoryIngestError.missingConversationID
        }
        guard value.count <= 256 else {
            throw MemoryIngestError.conversationIDTooLong
        }
        return value
    }
}

public enum MemoryIngestError: Error, Hashable, Sendable {
    case missingConversationID
    case conversationIDTooLong
    case negativeChunkIndex(Int)
    case invalidMessageRange(start: Int, end: Int)
    case duplicateChunkIndex(Int)
    case emptyReplacement
}

extension MemoryIngestError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .missingConversationID:
            "A conversation ID is required."
        case .conversationIDTooLong:
            "The conversation ID exceeds the 256-character partition limit."
        case .negativeChunkIndex(let value):
            "Chunk index \(value) cannot be negative."
        case .invalidMessageRange(let start, let end):
            "Message range \(start)…\(end) is invalid."
        case .duplicateChunkIndex(let value):
            "Chunk index \(value) appears more than once."
        case .emptyReplacement:
            "A normal memory replacement requires at least one chunk."
        }
    }
}
