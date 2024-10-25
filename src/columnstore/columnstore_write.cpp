#include "duckdb.hpp"

#include "duckdb/common/types/uuid.hpp"
#include "parquet_writer.hpp"

extern "C" {
#include "postgres.h"

#include "utils/rel.h"
}

#include "pgduckdb/pgduckdb_detoast.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_types.hpp"

#include "columnstore/columnstore.hpp"

#include "rust_extensions/delta.hpp"

class ParquetWriter {
public:
    ParquetWriter(duckdb::ClientContext &context, Oid oid, const char *path, duckdb::vector<duckdb::LogicalType> types,
                  duckdb::vector<duckdb::string> names)
        : m_oid(oid), m_file_name(path + duckdb::UUID::GenerateRandomUUID().ToString() + ".parquet"),
          m_collection(context, types, duckdb::ColumnDataAllocatorType::HYBRID),
          m_writer(context, duckdb::FileSystem::GetFileSystem(context), m_file_name, std::move(types), std::move(names),
                   duckdb_parquet::format::CompressionCodec::SNAPPY /*codec*/, {} /*field_ids*/, {} /*kv_metadata*/,
                   {} /*encryption_config*/, 1.0 /*dictionary_compression_ratio_threshold*/, {} /*compression_level*/,
                   true /*debug_use_openssl*/) {
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
        DataFilesAdd(m_oid, m_file_name.c_str());
        elog(NOTICE, "delta: %zu", delta("hello"));
    }

private:
    static const idx_t x_row_group_size = duckdb::Storage::ROW_GROUP_SIZE;
    static const idx_t x_row_group_size_bytes = x_row_group_size * 1024;

    Oid m_oid;
    std::string m_file_name;
    duckdb::ColumnDataCollection m_collection;
    duckdb::ColumnDataAppendState m_append_state;
    duckdb::ParquetWriter m_writer;
};

class ColumnstoreWriter {
public:
    void LazyInit(Oid oid, TupleDesc desc) {
        if (!m_con) {
            m_con = pgduckdb::DuckDBManager::CreateConnection();
        }
        duckdb::vector<duckdb::LogicalType> types;
        duckdb::vector<duckdb::string> names;
        for (int col = 0; col < desc->natts; col++) {
            Form_pg_attribute attr = &desc->attrs[col];
            types.push_back(pgduckdb::ConvertPostgresToDuckColumnType(attr));
            names.push_back(NameStr(attr->attname));
        }
        m_chunk.Initialize(*m_con->context, types);
        const char *path = TablesGet(oid);
        m_writer = duckdb::make_uniq<ParquetWriter>(*m_con->context, oid, path, std::move(types), std::move(names));
    }

    void CreateTable(Oid oid) {
        TablesAdd(oid, "" /*path*/);
    }

    void Insert(Relation table, TupleTableSlot **slots, int nslots) {
        TupleDesc desc = RelationGetDescr(table);
        if (!m_writer) {
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
                        pgduckdb::ConvertPostgresToDuckValue(desc->attrs[col].atttypid, value, vector, m_chunk.size());
                        if (should_free) {
                            duckdb_free(reinterpret_cast<void *>(value));
                        }
                    } else {
                        pgduckdb::ConvertPostgresToDuckValue(desc->attrs[col].atttypid, slot->tts_values[col], vector,
                                                             m_chunk.size());
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
        }
    }

private:
    duckdb::unique_ptr<duckdb::Connection> m_con;
    duckdb::DataChunk m_chunk;
    duckdb::unique_ptr<ParquetWriter> m_writer;
};

ColumnstoreWriter columnstore_writer;

void ColumnstoreCreateTable(Oid oid) {
    columnstore_writer.CreateTable(oid);
}

void ColumnstoreInsert(Relation table, TupleTableSlot **slots, int nslots) {
    columnstore_writer.Insert(table, slots, nslots);
}

void ColumnstoreFinalize() {
    columnstore_writer.Finalize();
}
