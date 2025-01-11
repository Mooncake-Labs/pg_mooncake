// Columnstore read cache filesystem implements read cache to optimize query performance.

#pragma once

#include <utility>

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class ColumnstoreReadCacheFileSystem : public FileSystem {
public:
    ColumnstoreReadCacheFileSystem(unique_ptr<FileSystem> internal_filesystem_p);
    std::string GetName() const override;

    // Open and read with read cache enabled if specified.
    unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
                                    optional_ptr<FileOpener> opener = nullptr) override;

    // For other API calls, delegate to [internal_filesystem] to handle.
    unique_ptr<FileHandle> OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) override {
        return internal_filesystem->OpenCompressedFile(std::move(handle), write);
    }
    void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
        internal_filesystem->Read(handle, buffer, nr_bytes, location);
    }
    int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
        return internal_filesystem->Read(handle, buffer, nr_bytes);
    }
    void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
        internal_filesystem->Write(handle, buffer, nr_bytes, location);
    }
    int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override {
        return internal_filesystem->Write(handle, buffer, nr_bytes);
    }
    bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override {
        return internal_filesystem->Trim(handle, offset_bytes, length_bytes);
    }
    int64_t GetFileSize(FileHandle &handle) {
        return internal_filesystem->GetFileSize(handle);
    }
    time_t GetLastModifiedTime(FileHandle &handle) override {
        return internal_filesystem->GetLastModifiedTime(handle);
    }
    FileType GetFileType(FileHandle &handle) override {
        return internal_filesystem->GetFileType(handle);
    }
    void Truncate(FileHandle &handle, int64_t new_size) override {
        return internal_filesystem->Truncate(handle, new_size);
    }
    bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
        return internal_filesystem->DirectoryExists(directory, opener);
    }
    void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
        internal_filesystem->CreateDirectory(directory, opener);
    }
    void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
        internal_filesystem->RemoveDirectory(directory, opener);
    }
    bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                   FileOpener *opener = nullptr) override {
        return internal_filesystem->ListFiles(directory, callback, opener);
    }
    void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override {
        internal_filesystem->MoveFile(source, target, opener);
    }
    bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
        return internal_filesystem->FileExists(filename, opener);
    }
    bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
        return internal_filesystem->IsPipe(filename, opener);
    }
    void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
        internal_filesystem->RemoveFile(filename, opener);
    }
    void FileSync(FileHandle &handle) override {
        internal_filesystem->FileSync(handle);
    }
    string GetHomeDirectory() override {
        return internal_filesystem->GetHomeDirectory();
    }
    string ExpandPath(const string &path) override {
        return internal_filesystem->ExpandPath(path);
    }
    string PathSeparator(const string &path) override {
        return internal_filesystem->PathSeparator(path);
    }
    vector<string> Glob(const string &path, FileOpener *opener = nullptr) override {
        return internal_filesystem->Glob(path, opener);
    }
    void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
        internal_filesystem->RegisterSubSystem(std::move(sub_fs));
    }
    void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override {
        internal_filesystem->RegisterSubSystem(compression_type, std::move(fs));
    }
    void UnregisterSubSystem(const string &name) override {
        internal_filesystem->UnregisterSubSystem(name);
    }
    vector<string> ListSubSystems() override {
        return internal_filesystem->ListSubSystems();
    }
    bool CanHandleFile(const string &fpath) override {
        return internal_filesystem->CanHandleFile(fpath);
    }
    void Seek(FileHandle &handle, idx_t location) override {
        internal_filesystem->Seek(handle, location);
    }
    void Reset(FileHandle &handle) override {
        internal_filesystem->Reset(handle);
    }
    idx_t SeekPosition(FileHandle &handle) override {
        return internal_filesystem->SeekPosition(handle);
    }
    bool IsManuallySet() override {
        return internal_filesystem->IsManuallySet();
    }
    bool CanSeek() override {
        return internal_filesystem->CanSeek();
    }
    bool OnDiskFile(FileHandle &handle) override {
        return internal_filesystem->OnDiskFile(handle);
    }
    void SetDisabledFileSystems(const vector<string> &names) override {
        internal_filesystem->SetDisabledFileSystems(names);
    }

private:
    // Return whether local filesystem has enough disk space to enable local read cache.
    bool HasEnoughSpaceForReadCache();
    // Cache the whole file specified by the given [path], which is a remote file.
    // If read and write fails, nothing happens.
    void CacheRemoteFileIfEnabled(const string &path, const FileOpenFlags &flags, optional_ptr<FileOpener> opener);

private:
    // Min requirement for disk space to enable read cache.
    constexpr static idx_t kMinDiskSpaceForCache = 1024 * 1024 * 1024;
    // Used to access local cache files.
    unique_ptr<FileSystem> local_filesystem;
    // Used to access remote files.
    unique_ptr<FileSystem> internal_filesystem;
};

// Wrap the given [internal_filesystem] with column store read cache filesystem if possible.
unique_ptr<FileSystem> WrapColumnstoreReadCacheFileSystem(unique_ptr<FileSystem> internal_filesystem);

} // namespace duckdb
