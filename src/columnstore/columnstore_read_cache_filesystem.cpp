#include "columnstore/columnstore_read_cache_filesystem.hpp"
#include "crypto.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector.hpp"
#include "pgmooncake_guc.hpp"

namespace duckdb {

namespace {

// Columnstore read cache filesystem name.
const string COLUMNSTORE_CACHE_FS_NAME = "ColumnstoreReadCacheFileSystem";
// Min required disk space to enable local read cache.
constexpr idx_t MIN_DISK_SPACE_FOR_CACHE = 1024ULL * 1024 * 1024;

// All read requests are split into chunks, and executed in parallel.
// A [CacheReadChunk] represents a chunked IO request and its corresponding
// partial IO request.
struct CacheReadChunk {
    // Requested memory address and file offset to read from for current chunk.
    char *requested_start_addr = nullptr;
    uint64_t requested_start_offset = 0;
    // Block size aligned [requested_start_offset].
    uint64_t aligned_start_offset = 0;

    // Number of bytes for the chunk for IO operations, apart from the last chunk
    // it's always cache block size.
    uint64_t chunk_size = 0;

    // Always allocate block size of memory for first and last chunk.
    // For middle chunks, if local cache is not hit, we also allocate memory for [content] as intermediate buffer.
    //
    // TODO(hjiang): For middle chunks, the performance could be improved further: remote IO operation directly read
    // into [requested_start_addr] then write to local cache file; but for code simplicity we also allocate here.
    string content;
    // Number of bytes to copy from [content] to requested memory address.
    uint64_t bytes_to_copy = 0;

    // Copy from [content] to application-provided buffer.
    void CopyBufferToRequestedMemory() {
        if (!content.empty()) {
            const uint64_t delta_offset = requested_start_offset - aligned_start_offset;
            std::memmove(requested_start_addr, const_cast<char *>(content.data()) + delta_offset, bytes_to_copy);
        }
    }
};

// Convert SHA256 value to hex string.
string Sha256ToHexString(const duckdb::hash_bytes &sha256) {
    static constexpr char kHexChars[] = "0123456789abcdef";
    std::string result;
    // SHA256 has 32 byte, we encode 2 chars for each byte of SHA256.
    result.reserve(64);

    for (unsigned char byte : sha256) {
        result += kHexChars[byte >> 4];  // Get high 4 bits
        result += kHexChars[byte & 0xF]; // Get low 4 bits
    }
    return result;
}

// Get local cache filename for the given [remote_file].
//
// Cache filename is formatted as `<cache-directory>/<filename-sha256>.<filename>`.
// So we could get all cache files under one directory, and get all cache files with commands like `ls`.
//
// Considering the naming format, it's worth noting it might _NOT_ work for local files, including mounted filesystems.
string GetLocalCacheFile(const string &remote_file, idx_t start_offset, idx_t bytes_to_read) {
    duckdb::hash_bytes remote_file_sha256_val;
    duckdb::sha256(remote_file.data(), remote_file.length(), remote_file_sha256_val);
    const string remote_file_sha256_str = Sha256ToHexString(remote_file_sha256_val);

    const string fname = StringUtil::GetFileName(remote_file);
    return StringUtil::Format("%s%s.%s-%llu-%llu", x_mooncake_local_cache, remote_file_sha256_str, fname, start_offset,
                              bytes_to_read);
}

// Attempt to cache [chunk] to local filesystem, if there's sufficient disk space available.
void CacheLocal(const CacheReadChunk &chunk, FileSystem &local_filesystem, const FileHandle &handle,
                const string &local_cache_file) {
    // Skip local cache if insufficient disk space.
    auto disk_space = local_filesystem.GetAvailableDiskSpace(x_mooncake_local_cache);
    if (!disk_space.IsValid() || disk_space.GetIndex() < MIN_DISK_SPACE_FOR_CACHE) {
        return;
    }

    // Dump to a temporary location at local filesystem.
    const auto fname = StringUtil::GetFileName(handle.GetPath());
    const auto local_temp_file = StringUtil::Format("%s%s.%s.httpfs_local_cache", x_mooncake_local_cache, fname,
                                                    UUID::ToString(UUID::GenerateRandomUUID()));
    {
        auto file_handle = local_filesystem.OpenFile(local_temp_file, FileOpenFlags::FILE_FLAGS_WRITE |
                                                                          FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
        local_filesystem.Write(*file_handle, const_cast<char *>(chunk.content.data()),
                               /*nr_bytes=*/chunk.content.length(),
                               /*location=*/0);
        file_handle->Sync();
    }

    // Then atomically move to the target postion to prevent data corruption due
    // to concurrent write.
    local_filesystem.MoveFile(/*source=*/local_temp_file,
                              /*target=*/local_cache_file);
}

} // namespace

DiskCacheFileHandle::DiskCacheFileHandle(unique_ptr<FileHandle> internal_file_handle_p,
                                         ColumnstoreReadCacheFileSystem &fs)
    : FileHandle(fs, internal_file_handle_p->GetPath()), internal_file_handle(std::move(internal_file_handle_p)) {}

ColumnstoreReadCacheFileSystem::ColumnstoreReadCacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p,
                                                               OnDiskCacheConfig cache_config_p)
    : cache_config(std::move(cache_config_p)), local_filesystem(LocalFileSystem::CreateLocal()),
      internal_filesystem(std::move(internal_filesystem_p)) {}

std::string ColumnstoreReadCacheFileSystem::GetName() const {
    return COLUMNSTORE_CACHE_FS_NAME;
}

void ColumnstoreReadCacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
    ReadImpl(handle, buffer, nr_bytes, location, cache_config.block_size);
}
int64_t ColumnstoreReadCacheFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
    const int64_t bytes_read = ReadImpl(handle, buffer, nr_bytes, handle.SeekPosition(), cache_config.block_size);
    handle.Seek(handle.SeekPosition() + bytes_read);
    return bytes_read;
}

void ColumnstoreReadCacheFileSystem::ReadAndCache(FileHandle &handle, char *buffer, uint64_t requested_start_offset,
                                                  uint64_t requested_bytes_to_read, uint64_t file_size) {
    const idx_t block_size = cache_config.block_size;
    const uint64_t aligned_start_offset = requested_start_offset / block_size * block_size;
    const uint64_t aligned_last_chunk_offset =
        (requested_start_offset + requested_bytes_to_read) / block_size * block_size;

    // Indicate the meory address to copy to for each IO operation
    char *addr_to_write = buffer;
    // Used to calculate bytes to copy for last chunk.
    uint64_t already_read_bytes = 0;
    // Threads to parallelly perform IO.
    vector<thread> io_threads;

    // To improve IO performance, we split requested bytes (after alignment) into
    // multiple chunks and fetch them in parallel.
    for (uint64_t io_start_offset = aligned_start_offset; io_start_offset <= aligned_last_chunk_offset;
         io_start_offset += block_size) {
        CacheReadChunk cache_read_chunk;
        cache_read_chunk.requested_start_addr = addr_to_write;
        cache_read_chunk.aligned_start_offset = io_start_offset;
        cache_read_chunk.requested_start_offset = requested_start_offset;

        // Implementation-wise, middle chunks are easy to handle -- read in [block_size], and copy the whole chunk to
        // the requested memory address; but the first and last chunk require special handling.
        // For first chunk, requested start offset might not be aligned with block size; for the last chunk, we might
        // not need to copy the whole [block_size] of memory.
        //
        // Case-1: If there's only one chunk, which serves as both the first chunk and the last one.
        if (io_start_offset == aligned_start_offset && io_start_offset == aligned_last_chunk_offset) {
            cache_read_chunk.chunk_size = std::min<uint64_t>(block_size, file_size - io_start_offset);
            cache_read_chunk.content = std::string(cache_read_chunk.chunk_size, '\0');
            cache_read_chunk.bytes_to_copy = requested_bytes_to_read;
        }
        // Case-2: First chunk.
        else if (io_start_offset == aligned_start_offset) {
            const uint64_t delta_offset = requested_start_offset - aligned_start_offset;
            addr_to_write += block_size - delta_offset;
            already_read_bytes += block_size - delta_offset;

            cache_read_chunk.chunk_size = block_size;
            cache_read_chunk.content = std::string(block_size, '\0');
            cache_read_chunk.bytes_to_copy = block_size - delta_offset;
        }
        // Case-3: Last chunk.
        else if (io_start_offset == aligned_last_chunk_offset) {
            cache_read_chunk.chunk_size = std::min<uint64_t>(block_size, file_size - io_start_offset);
            cache_read_chunk.content = std::string(cache_read_chunk.chunk_size, '\0');
            cache_read_chunk.bytes_to_copy = requested_bytes_to_read - already_read_bytes;
        }
        // Case-4: Middle chunks.
        else {
            addr_to_write += block_size;
            already_read_bytes += block_size;

            cache_read_chunk.bytes_to_copy = block_size;
            cache_read_chunk.chunk_size = block_size;
        }

        // Update read offset for next chunk read.
        requested_start_offset = io_start_offset + block_size;

        // Perform read operation in parallel.
        io_threads.emplace_back([this, &handle, block_size, cache_read_chunk = std::move(cache_read_chunk)]() mutable {
            // Check local cache first, see if we could do a cached read.
            const auto local_cache_file =
                GetLocalCacheFile(handle.GetPath(), cache_read_chunk.aligned_start_offset, cache_read_chunk.chunk_size);

            if (local_filesystem->FileExists(local_cache_file)) {
                auto file_handle = local_filesystem->OpenFile(local_cache_file, FileOpenFlags::FILE_FLAGS_READ);
                void *addr = !cache_read_chunk.content.empty() ? const_cast<char *>(cache_read_chunk.content.data())
                                                               : cache_read_chunk.requested_start_addr;
                local_filesystem->Read(*file_handle, addr, cache_read_chunk.chunk_size,
                                       /*location=*/0);
                cache_read_chunk.CopyBufferToRequestedMemory();
                return;
            }

            // We suffer a cache loss, fallback to remote access then local filesystem write.
            if (cache_read_chunk.content.empty()) {
                cache_read_chunk.content = std::string(cache_read_chunk.chunk_size, '\0');
            }
            auto &disk_cache_handle = handle.Cast<DiskCacheFileHandle>();
            internal_filesystem->Read(*disk_cache_handle.internal_file_handle,
                                      const_cast<char *>(cache_read_chunk.content.data()),
                                      cache_read_chunk.content.length(), cache_read_chunk.aligned_start_offset);

            // Copy to destination buffer.
            cache_read_chunk.CopyBufferToRequestedMemory();

            // Attempt to cache file locally.
            CacheLocal(cache_read_chunk, *local_filesystem, handle, local_cache_file);
        });
    }
    for (auto &cur_thd : io_threads) {
        D_ASSERT(cur_thd.joinable());
        cur_thd.join();
    }
}

int64_t ColumnstoreReadCacheFileSystem::ReadImpl(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location,
                                                 uint64_t block_size) {
    const auto file_size = handle.GetFileSize();

    // No more bytes to read.
    if (location == file_size) {
        return 0;
    }

    const int64_t bytes_to_read = std::min<int64_t>(nr_bytes, file_size - location);
    ReadAndCache(handle, static_cast<char *>(buffer), location, bytes_to_read, file_size);

    return bytes_to_read;
}

} // namespace duckdb
