//
//  FeatherStorageEphemeralTestSuite.swift
//  feather-storage-ephemeral
//
//  Created by Tibor Bödecs on 2023. 01. 16.

import FeatherStorage
import NIOCore
import Testing

@testable import FeatherStorageEphemeral

@Suite
struct FeatherStorageEphemeralTestSuite {

    private func upload(
        storage: StorageClientEphemeral,
        key: String,
        text: String
    ) async throws {
        try await storage.upload(
            key: key,
            sequence: makeSequence(from: text)
        )
    }

    private func makeSequence(from text: String) -> StorageSequence {
        var buffer = ByteBufferAllocator().buffer(capacity: text.utf8.count)
        buffer.writeString(text)
        return StorageSequence(
            asyncSequence: ByteBufferSequence(buffer: buffer),
            length: UInt64(buffer.readableBytes)
        )
    }

    private func collect(
        sequence: StorageSequence
    ) async throws -> ByteBuffer {
        var result = ByteBufferAllocator()
            .buffer(capacity: Int(sequence.length ?? 0))
        for try await chunk in sequence {
            var chunk = chunk
            result.writeBuffer(&chunk)
        }
        return result
    }

    private func string(from buffer: ByteBuffer) -> String? {
        buffer.getString(at: buffer.readerIndex, length: buffer.readableBytes)
    }

    // MARK: - tests

    @Test
    func uploadDownloadRangeAndMetadata() async throws {
        let storage = StorageClientEphemeral()

        try await upload(
            storage: storage,
            key: "docs/hello.txt",
            text: "hello-ephemeral"
        )

        #expect(try await storage.exists(key: "docs/hello.txt") == true)
        #expect(try await storage.size(key: "docs/hello.txt") == 15)

        let downloaded = try await collect(
            sequence: storage.download(key: "docs/hello.txt", range: nil)
        )
        #expect(string(from: downloaded) == "hello-ephemeral")

        let ranged = try await collect(
            sequence: storage.download(key: "docs/hello.txt", range: 6...14)
        )
        #expect(string(from: ranged) == "ephemeral")

        do {
            _ = try await storage.download(
                key: "docs/hello.txt",
                range: 99...100
            )
            Issue.record("Expected invalidBuffer")
        }
        catch StorageClientError.invalidBuffer {}
        catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test
    func listCreateDelete() async throws {
        let storage = StorageClientEphemeral()

        try await storage.create(key: "docs/new")
        try await upload(storage: storage, key: "docs/new/a.txt", text: "A")
        try await upload(storage: storage, key: "docs/new/b.txt", text: "B")

        #expect(try await storage.list(key: "docs") == ["new"])
        #expect(
            try await storage.list(key: "docs/new").sorted() == [
                "a.txt", "b.txt",
            ]
        )

        try await storage.delete(key: "docs/new")
        #expect(try await storage.exists(key: "docs/new/a.txt") == false)
        #expect(try await storage.list(key: "docs/new").isEmpty)
    }

    @Test
    func multipartLifecycle() async throws {
        let storage = StorageClientEphemeral()

        let uploadId = try await storage.createMultipartId(
            key: "docs/multipart.txt"
        )
        let part1 = try await storage.upload(
            multipartId: uploadId,
            key: "docs/multipart.txt",
            number: 1,
            sequence: makeSequence(from: "chunk-")
        )
        let part2 = try await storage.upload(
            multipartId: uploadId,
            key: "docs/multipart.txt",
            number: 2,
            sequence: makeSequence(from: "done")
        )
        try await storage.finish(
            multipartId: uploadId,
            key: "docs/multipart.txt",
            chunks: [part1, part2]
        )

        let multipart = try await collect(
            sequence: storage.download(key: "docs/multipart.txt", range: nil)
        )
        #expect(string(from: multipart) == "chunk-done")
    }

    @Test
    func abortInvalidatesMultipartId() async throws {
        let storage = StorageClientEphemeral()
        let uploadId = try await storage.createMultipartId(
            key: "docs/aborted.txt"
        )

        try await storage.abort(multipartId: uploadId, key: "docs/aborted.txt")

        do {
            _ = try await storage.upload(
                multipartId: uploadId,
                key: "docs/aborted.txt",
                number: 1,
                sequence: makeSequence(from: "x")
            )
            Issue.record("Expected invalidMultipartId")
        }
        catch StorageClientError.invalidMultipartId {}
        catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}
