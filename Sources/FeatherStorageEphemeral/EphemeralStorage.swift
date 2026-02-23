//
//  EphemeralStorage.swift
//  feather-storage-ephemeral
//
//  Created by Tibor Bödecs on 2023. 01. 16.

import FeatherStorage
import NIOCore

actor EphemeralStorage {

    private var objects: [String: ByteBuffer] = [:]
    private var directories: Set<String> = []
    private var multipartUploads: [String: MultipartUpload] = [:]

    struct MultipartUpload {
        let key: String
        var parts: [Int: StoredPart]
    }

    struct StoredPart {
        let chunkId: String
        let buffer: ByteBuffer
    }

    init() {
        directories.insert("")
    }

    func putObject(_ key: String, buffer: ByteBuffer) {
        objects[key] = buffer
        ensureParentDirectories(for: key)
    }

    func object(for key: String) -> ByteBuffer? {
        objects[key]
    }

    func objectExists(_ key: String) -> Bool {
        objects[key] != nil
            || directories.contains(Self.normalizeDirectoryKey(key))
    }

    func objectSize(
        _ key: String
    ) -> UInt64 {
        UInt64(objects[key]?.readableBytes ?? 0)
    }

    func remove(
        _ key: String
    ) {
        let normalizedDirectory = Self.normalizeDirectoryKey(key)
        objects = objects.filter { objectKey, _ in
            !(objectKey == key
                || objectKey.hasPrefix(normalizedDirectory + "/"))
        }
        directories = directories.filter { directoryKey in
            !(directoryKey == normalizedDirectory
                || directoryKey.hasPrefix(normalizedDirectory + "/"))
        }
    }

    func copyObject(
        source: String,
        destination: String
    ) throws(StorageClientError) {
        guard let sourceObject = objects[source] else {
            throw .invalidKey
        }
        objects[destination] = sourceObject
        ensureParentDirectories(for: destination)
    }

    func createDirectory(
        _ key: String
    ) {
        let normalized = Self.normalizeDirectoryKey(key)
        directories.insert(normalized)
        ensureParentDirectories(for: normalized)
    }

    func list(
        prefix: String?
    ) -> [String] {
        let normalizedPrefix = prefix.map(Self.normalizeDirectoryKey)
        let rootPrefix =
            normalizedPrefix.map { $0.isEmpty ? "" : $0 + "/" } ?? ""

        var entries: Set<String> = []

        for objectKey in objects.keys where objectKey.hasPrefix(rootPrefix) {
            let remainder = String(objectKey.dropFirst(rootPrefix.count))
            guard !remainder.isEmpty else { continue }
            let component = remainder.split(separator: "/", maxSplits: 1).first
                .map(String.init)
            if let component {
                entries.insert(component)
            }
        }

        for directory in directories where directory.hasPrefix(rootPrefix) {
            let remainder = String(directory.dropFirst(rootPrefix.count))
            guard !remainder.isEmpty else { continue }
            let component = remainder.split(separator: "/", maxSplits: 1).first
                .map(String.init)
            if let component {
                entries.insert(component)
            }
        }

        return entries.sorted()
    }

    func createMultipartUpload(
        for key: String
    ) -> String {
        let id = "upload-\(UInt64.random(in: .min ... .max))"
        multipartUploads[id] = .init(key: key, parts: [:])
        ensureParentDirectories(for: key)
        return id
    }

    func addMultipartPart(
        uploadId: String,
        key: String,
        number: Int,
        buffer: ByteBuffer
    ) throws(StorageClientError) -> StorageMultipartChunk {
        guard number > 0 else {
            throw .invalidMultipartChunk
        }
        guard var upload = multipartUploads[uploadId], upload.key == key else {
            throw .invalidMultipartId
        }

        let chunkId = "chunk-\(UInt64.random(in: .min ... .max))"
        upload.parts[number] = .init(chunkId: chunkId, buffer: buffer)
        multipartUploads[uploadId] = upload

        return .init(id: chunkId, number: number)
    }

    func abortMultipartUpload(
        uploadId: String,
        key: String
    ) throws(StorageClientError) {
        guard let upload = multipartUploads[uploadId], upload.key == key else {
            throw .invalidMultipartId
        }
        _ = upload
        multipartUploads.removeValue(forKey: uploadId)
    }

    func finishMultipartUpload(
        uploadId: String,
        key: String,
        chunks: [StorageMultipartChunk]
    ) throws(StorageClientError) {
        guard let upload = multipartUploads[uploadId], upload.key == key else {
            throw .invalidMultipartId
        }

        var result = ByteBufferAllocator().buffer(capacity: 0)
        for chunk in chunks.sorted(by: { $0.number < $1.number }) {
            guard let part = upload.parts[chunk.number],
                part.chunkId == chunk.id
            else {
                throw .invalidMultipartChunk
            }
            var buffer = part.buffer
            result.writeBuffer(&buffer)
        }

        objects[key] = result
        ensureParentDirectories(for: key)
        multipartUploads.removeValue(forKey: uploadId)
    }

    private func ensureParentDirectories(
        for key: String
    ) {
        let parts = key.split(separator: "/").map(String.init)
        guard parts.count > 1 else { return }

        var current = ""
        for part in parts.dropLast() {
            current = current.isEmpty ? part : current + "/" + part
            directories.insert(current)
        }
    }

    private static func normalizeDirectoryKey(
        _ key: String
    ) -> String {
        let components = key.split(separator: "/").filter { !$0.isEmpty }
        return components.joined(separator: "/")
    }
}
