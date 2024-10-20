#include "duckdb.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "httpfs_extension.hpp"
#include "parquet_writer.hpp"

extern "C" {
#include "postgres.h"

#include "utils/rel.h"
}

#include "pgduckdb/pgduckdb_detoast.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

#include "columnstore/columnstore.hpp"
#include "lake/lake.hpp"

namespace duckdb
{
class SingleFileCachedWriteFileSystem : public FileSystem {
public:
    SingleFileCachedWriteFileSystem(ClientContext &context, const std::string& file_name)
        :tmp_file_path("mooncake_tmp/" + file_name), file_size(0), internal(GetFileSystem(context)){}

    unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr)
    {
        //if (FileSystem::IsRemoteFile(path))
        {
            tmp_file = internal.OpenFile(tmp_file_path, flags, opener);
        }
        return internal.OpenFile(path, flags, opener);
    }                                   
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override{
        file_size += nr_bytes;
        if (tmp_file) {
            int64_t written = internal.Write(*tmp_file, buffer, nr_bytes);
            assert(written == nr_bytes);
        }
        return internal.Write(handle, buffer, nr_bytes);
    }
    void CacheFile(int64_t file_id) {
        if (tmp_file) {
            tmp_file->Close();
            std::rename(tmp_file_path.c_str(), duckdb::StringUtil::Format("mooncake_cache/%ld.parquet", file_id).c_str());
        }
    }
    std::string GetName() const { return "SingleFileCachedWriteFileSystem";}
    idx_t GetCurrentFileSize() const { return file_size; }
private:
    std::string tmp_file_path;
    idx_t file_size;
    unique_ptr<FileHandle> tmp_file;
    FileSystem& internal;
};
}

class ParquetWriter {
  public:
    ParquetWriter(duckdb::ClientContext &context, Oid oid, std::string path, duckdb::vector<duckdb::LogicalType> types,
                  duckdb::vector<duckdb::string> names, duckdb::ChildFieldIDs fieldIds)
        : m_context(context), m_oid(oid), m_path(std::move(path)),
          m_file_name(duckdb::UUID::GenerateRandomUUID().ToString() + ".parquet"),
          m_file_system(context, m_file_name),
          m_collection(context, types, duckdb::ColumnDataAllocatorType::HYBRID),
          m_writer(context, m_file_system, m_path + m_file_name, std::move(types),
                   std::move(names), duckdb_parquet::format::CompressionCodec::SNAPPY /*codec*/, std::move(fieldIds),
                   {} /*kv_metadata*/, {} /*encryption_config*/, 1.0 /*dictionary_compression_ratio_threshold*/,
                   {} /*compression_level*/, true /*debug_use_openssl*/) {
        m_collection.InitializeAppend(m_append_state);
    }

    void Append(duckdb::DataChunk &chunk) {
        m_collection.Append(m_append_state, chunk);
        if (m_collection.Count() >= x_row_group_size || m_collection.SizeInBytes() >= x_row_group_size_bytes) {
            m_writer.Flush(m_collection);
            m_append_state.current_chunk_state.handles.clear();
            m_collection.InitializeAppend(m_append_state);
        }
    }

    void Finalize() {
        m_writer.Flush(m_collection);
        m_writer.Finalize();
        int64_t file_id = DataFilesAdd(m_oid, (m_path + m_file_name).c_str());
        CacheAdd(m_oid, file_id);
        m_file_system.CacheFile(file_id);
        duckdb::idx_t size = m_file_system.GetCurrentFileSize();
        LakeAddFile(m_oid, m_file_name.c_str(), size);
    }

  private:
    static const idx_t x_row_group_size = duckdb::Storage::ROW_GROUP_SIZE;
    static const idx_t x_row_group_size_bytes = x_row_group_size * 1024;

    duckdb::ClientContext &m_context;
    Oid m_oid;
    std::string m_path;
    std::string m_file_name;
    duckdb::SingleFileCachedWriteFileSystem m_file_system;
    duckdb::ColumnDataCollection m_collection;
    duckdb::ColumnDataAppendState m_append_state;
    duckdb::ParquetWriter m_writer;
};

class ColumnstoreWriter {
  public:
    ColumnstoreWriter() : m_con(pgduckdb::DuckDBManager::Get().GetDatabase()) {
        pgduckdb::DuckDBManager::Get().GetDatabase().LoadStaticExtension<duckdb::HttpfsExtension>();
    }

    void LazyInit(Oid oid, TupleDesc desc) {
        duckdb::vector<duckdb::LogicalType> types;
        duckdb::vector<duckdb::string> names;
        duckdb::ChildFieldIDs fieldIds;
        for (int col = 0; col < desc->natts; col++) {
            Form_pg_attribute attr = &desc->attrs[col];
            types.push_back(pgduckdb::ConvertPostgresToDuckColumnType(attr));
            names.push_back(NameStr(attr->attname));
            fieldIds.ids->insert(std::make_pair(names[col], duckdb::FieldID(col)));
        }
        m_chunk.Initialize(*m_con.context, types);
        ColumnstoreOptions options = TablesGet(oid);
        m_writer = duckdb::make_uniq<ParquetWriter>(*m_con.context, oid, std::move(options.path), std::move(types),
                                                    std::move(names), std::move(fieldIds));
    }

    void CreateTable(Oid oid, const ColumnstoreOptions &options) {
        if (strlen(options.path) && !duckdb::FileSystem::IsRemoteFile(options.path)) {
            duckdb::FileSystem::GetFileSystem(*m_con.context).CreateDirectory(options.path);
        }
        LakeCreateTable(oid, options.path);
        TablesAdd(oid, options);
    }

    void Insert(Relation table, TupleTableSlot **slots, int nslots) {
        TupleDesc desc = RelationGetDescr(table);
        if (!m_writer) {
            m_con.BeginTransaction();
            pgduckdb::DuckDBManager::Get().LoadSecretsIfNeeded();
            LazyInit(RelationGetRelid(table), desc);
        }

        for (int row = 0; row < nslots; row++) {
            TupleTableSlot *slot = slots[row];
            for (int col = 0; col < desc->natts; col++) {
                auto &vector = m_chunk.data[col];
                if (slot->tts_isnull[col]) {
                    duckdb::FlatVector::Validity(vector).SetInvalid(m_chunk.size());
                } else {
                    if (desc->attrs[col].attlen == -1) {
                        bool should_free = false;
                        Datum value = pgduckdb::DetoastPostgresDatum(reinterpret_cast<varlena *>(slot->tts_values[col]),
                                                                     &should_free);
                        pgduckdb::ConvertPostgresToDuckValue(value, vector, m_chunk.size());
                        if (should_free) {
                            duckdb_free(reinterpret_cast<void *>(value));
                        }
                    } else {
                        pgduckdb::ConvertPostgresToDuckValue(slot->tts_values[col], vector, m_chunk.size());
                    }
                }
            }

            m_chunk.SetCardinality(m_chunk.size() + 1);
            if (m_chunk.size() == STANDARD_VECTOR_SIZE) {
                Flush();
            }
        }
    }

    void Flush() {
        m_chunk.Verify();
        m_writer->Append(m_chunk);
        m_chunk.Reset();
    }

    void Finalize() {
        if (m_writer) {
            Flush();
            m_writer->Finalize();
            m_chunk.Destroy();
            m_writer.reset();
            m_con.Commit();
        }
    }

  private:
    duckdb::Connection m_con;
    duckdb::DataChunk m_chunk;
    duckdb::unique_ptr<ParquetWriter> m_writer;
};

// DevNote: this will be created before Create Extension finishes!
//
ColumnstoreWriter columnstore_writer;

void ColumnstoreCreateTable(Oid oid, const ColumnstoreOptions &options) {
    columnstore_writer.CreateTable(oid, options);
}

void ColumnstoreInsert(Relation table, TupleTableSlot **slots, int nslots) {
    columnstore_writer.Insert(table, slots, nslots);
}

void ColumnstoreFinalize() {
    columnstore_writer.Finalize();
}
