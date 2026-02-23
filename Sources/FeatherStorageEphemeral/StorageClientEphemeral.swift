//
//  StorageClientEphemeral.swift
//  feather-storage-emphemeral
//
//  Created by Tibor Bödecs on 2023. 01. 16.

import FeatherStorage
import NIOCore

/// In-memory storage driver intended for tests, previews, and local development.
public struct StorageClientEphemeral: StorageClient {
    private let storage: EphemeralStorage

    /// Creates a new in-memory storage client with isolated state.
    public init() {
        self.storage = .init()
    }

    init(storage: EphemeralStorage) {
        self.storage = storage
    }

    /// Uploads an object for the given key.
    ///
    /// - Parameters:
    ///   - key: The destination object key.
    ///   - sequence: The async byte sequence to store.
    /// - Throws: ``StorageClientError`` if reading or storing the object fails.
    public func upload(
        key: String,
        sequence: StorageSequence
    ) async throws(StorageClientError) {
        do {
            let object = try await sequence.collect(upTo: .max)
            return await storage.putObject(key, buffer: object)
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Downloads an object for the given key.
    ///
    /// - Parameters:
    ///   - key: The source object key.
    ///   - range: An optional inclusive byte range to return.
    /// - Returns: A storage sequence containing either the full object or the requested range.
    /// - Throws: ``StorageClientError`` if the key or range is invalid.
    public func download(
        key: String,
        range: ClosedRange<Int>?
    ) async throws(StorageClientError) -> StorageSequence {
        guard var object = await storage.object(for: key) else {
            throw .invalidKey
        }

        if let range {
            guard range.lowerBound >= 0, range.upperBound < object.readableBytes
            else {
                throw .invalidBuffer
            }
            object.moveReaderIndex(to: range.lowerBound)
            let length = range.upperBound - range.lowerBound + 1
            guard let bytes = object.readBytes(length: length) else {
                throw .invalidBuffer
            }
            object = ByteBuffer(bytes: bytes)
        }

        return StorageSequence(
            asyncSequence: ByteBufferSequence(buffer: object),
            length: UInt64(object.readableBytes)
        )
    }

    /// Checks whether an object or directory exists for the given key.
    ///
    /// - Parameter key: The key to check.
    /// - Returns: `true` if an object or directory exists, otherwise `false`.
    /// - Throws: ``StorageClientError`` if the existence check fails.
    public func exists(
        key: String
    ) async throws(StorageClientError) -> Bool {
        await storage.objectExists(key)
    }

    /// Returns the size of the object at the given key.
    ///
    /// - Parameter key: The object key.
    /// - Returns: The object size in bytes.
    /// - Throws: ``StorageClientError`` if retrieving the size fails.
    public func size(
        key: String
    ) async throws(StorageClientError) -> UInt64 {
        await storage.objectSize(key)
    }

    /// Copies an object from one key to another.
    ///
    /// - Parameters:
    ///   - source: The source object key.
    ///   - destination: The destination object key.
    /// - Throws: ``StorageClientError`` if the source key is invalid.
    public func copy(
        key source: String,
        to destination: String
    ) async throws(StorageClientError) {
        try await storage.copyObject(source: source, destination: destination)
    }

    /// Lists child entries under an optional prefix.
    ///
    /// - Parameter key: Optional prefix key to list under.
    /// - Returns: Immediate child names under the prefix.
    /// - Throws: ``StorageClientError`` if listing fails.
    public func list(
        key: String?
    ) async throws(StorageClientError) -> [String] {
        await storage.list(prefix: key)
    }

    /// Deletes an object or directory tree at the given key.
    ///
    /// - Parameter key: The key to delete.
    /// - Throws: ``StorageClientError`` if deletion fails.
    public func delete(
        key: String
    ) async throws(StorageClientError) {
        await storage.remove(key)
    }

    /// Creates a directory at the given key.
    ///
    /// - Parameter key: The directory key to create.
    /// - Throws: ``StorageClientError`` if directory creation fails.
    public func create(
        key: String
    ) async throws(StorageClientError) {
        await storage.createDirectory(key)
    }

    /// Creates a new multipart upload identifier for a key.
    ///
    /// - Parameter key: The destination object key.
    /// - Returns: A new multipart upload identifier.
    /// - Throws: ``StorageClientError`` if multipart setup fails.
    public func createMultipartId(
        key: String
    ) async throws(StorageClientError) -> String {
        await storage.createMultipartUpload(for: key)
    }

    /// Uploads a multipart chunk for an existing multipart upload.
    ///
    /// - Parameters:
    ///   - multipartId: The multipart upload identifier.
    ///   - key: The destination object key.
    ///   - number: The 1-based chunk number.
    ///   - sequence: The chunk bytes to upload.
    /// - Returns: Metadata for the stored multipart chunk.
    /// - Throws: ``StorageClientError`` if the multipart upload, chunk number, or payload is invalid.
    public func upload(
        multipartId: String,
        key: String,
        number: Int,
        sequence: StorageSequence
    ) async throws(StorageClientError) -> StorageMultipartChunk {
        do {
            let part = try await sequence.collect(upTo: .max)
            return try await storage.addMultipartPart(
                uploadId: multipartId,
                key: key,
                number: number,
                buffer: part
            )
        }
        catch let error as StorageClientError {
            throw error
        }
        catch {
            throw .unknown(error)
        }
    }

    /// Aborts an in-progress multipart upload.
    ///
    /// - Parameters:
    ///   - multipartId: The multipart upload identifier.
    ///   - key: The destination object key.
    /// - Throws: ``StorageClientError`` if the multipart upload identifier is invalid.
    public func abort(
        multipartId: String,
        key: String
    ) async throws(StorageClientError) {
        try await storage.abortMultipartUpload(uploadId: multipartId, key: key)
    }

    /// Finalizes a multipart upload and assembles the full object.
    ///
    /// - Parameters:
    ///   - multipartId: The multipart upload identifier.
    ///   - key: The destination object key.
    ///   - chunks: The ordered chunk metadata to assemble.
    /// - Throws: ``StorageClientError`` if the multipart upload or chunk list is invalid.
    public func finish(
        multipartId: String,
        key: String,
        chunks: [StorageMultipartChunk]
    ) async throws(StorageClientError) {
        try await storage.finishMultipartUpload(
            uploadId: multipartId,
            key: key,
            chunks: chunks
        )
    }
}
