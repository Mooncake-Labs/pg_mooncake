use std::collections::HashMap;

use cxx::{CxxString, CxxVector};
use deltalake::aws::register_handlers;
use deltalake::kernel::{Action, Add, ArrayType, DataType, PrimitiveType, Remove, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{open_table_with_storage_options, TableProperty};

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn DeltaInit();
        fn DeltaCreateTable(
            table_name: &CxxString,
            location: &CxxString,
            storage_option: &CxxString,
            column_names: &CxxVector<CxxString>,
            column_types: &CxxVector<CxxString>,
        ) -> Result<()>;

        fn DeltaModifyFiles(
            table_location: &CxxString,
            storage_option: &CxxString,
            file_paths: &CxxVector<CxxString>,
            file_sizes: &CxxVector<i64>,
            is_add_files: &CxxVector<i8>,
        ) -> Result<()>;
    }
}

#[allow(non_snake_case)]
pub fn DeltaInit() {
    // Register S3 handlers
    //
    register_handlers(None);
}

#[allow(non_snake_case)]
pub fn DeltaCreateTable(
    table_name: &CxxString,
    location: &CxxString,
    storage_option: &CxxString,
    column_names: &CxxVector<CxxString>,
    column_types: &CxxVector<CxxString>,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let metadata = vec![(
            "creator".to_string(),
            serde_json::json!("pg_mooncake_extension"),
        )];
        let mut storage_option_map: HashMap<String, String> =
            serde_json::from_str(storage_option.to_str()?).expect("invalid storage options");
        // Write directly to S3 without locking is safe since Mooncake will be the only writer.
        //
        storage_option_map.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
        let _table = CreateBuilder::new()
            .with_location(location.to_str()?)
            .with_storage_options(storage_option_map)
            .with_table_name(table_name.to_str()?)
            .with_configuration_property(TableProperty::MinReaderVersion, Some("3"))
            .with_configuration_property(TableProperty::MinWriterVersion, Some("7"))
            .with_columns(map_postgres_columns(column_names, column_types))
            .with_metadata(metadata)
            .with_save_mode(SaveMode::ErrorIfExists)
            .await?;
        Ok(())
    })
}

#[allow(non_snake_case)]
pub fn DeltaModifyFiles(
    table_location: &CxxString,
    storage_option: &CxxString,
    file_paths: &CxxVector<CxxString>,
    file_sizes: &CxxVector<i64>,
    is_add_files: &CxxVector<i8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let mut storage_option_map: HashMap<String, String> =
            serde_json::from_str(storage_option.to_str()?).expect("invalid storage options");
        // Write directly to S3 without locking is safe since Mooncake will be the only writer.
        //
        storage_option_map.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
        let mut table: deltalake::DeltaTable =
            open_table_with_storage_options(table_location.to_string(), storage_option_map).await?;
        let mut actions = Vec::new();
        for ((file_path, file_size), is_add) in file_paths
            .iter()
            .zip(file_sizes.iter())
            .zip(is_add_files.iter())
        {
            if *is_add == 1 {
                let add = Add {
                    path: file_path.to_string(),
                    size: *file_size,
                    data_change: true,
                    ..Default::default()
                };
                actions.push(Action::Add(add));
            } else {
                let rm = Remove {
                    path: file_path.to_string(),
                    data_change: true,
                    ..Default::default()
                };
                actions.push(Action::Remove(rm));
            }
        }
        let op = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };
        CommitBuilder::default()
            .with_actions(actions)
            .build(Some(table.snapshot()?), table.log_store().clone(), op)
            .await?;
        table.update().await?;
        Ok(())
    })
}

// Helper functions
//

pub fn map_postgres_columns(
    column_names: &CxxVector<CxxString>,
    column_types: &CxxVector<CxxString>,
) -> Vec<StructField> {
    column_names
        .into_iter()
        .zip(column_types.into_iter())
        .map(|(name, pg_type)| {
            StructField::new(
                name.to_string(),
                map_pg_type_to_delta(&pg_type.to_string()),
                true, // Assuming all columns are nullable for simplicity
            )
        })
        .collect()
}

fn map_pg_type_to_delta(pg_type: &str) -> DataType {
    match pg_type {
        "smallint" => DataType::Primitive(PrimitiveType::Short),
        "integer" => DataType::Primitive(PrimitiveType::Integer),
        "bigint" => DataType::Primitive(PrimitiveType::Long),
        "real" => DataType::Primitive(PrimitiveType::Float),
        "double precision" => DataType::Primitive(PrimitiveType::Double),
        "boolean" => DataType::Primitive(PrimitiveType::Boolean),
        "character varying" | "text" => DataType::Primitive(PrimitiveType::String),
        "date" => DataType::Primitive(PrimitiveType::Date),
        "timestamp without time zone" => DataType::Primitive(PrimitiveType::TimestampNtz),
        "timestamp with time zone" => DataType::Primitive(PrimitiveType::Timestamp),
        "time without time zone" | "time with time zone" => {
            DataType::Primitive(PrimitiveType::String)
        }
        "numeric" | "decimal" => DataType::Primitive(PrimitiveType::Decimal(38, 10)), // Default precision and scale
        "bytea" => DataType::Primitive(PrimitiveType::Binary),
        _ if pg_type.ends_with("[]") => {
            let base_type = &pg_type[..pg_type.len() - 2];
            DataType::from(ArrayType::new(map_pg_type_to_delta(base_type), true))
        }
        _ => DataType::Primitive(PrimitiveType::String), // Default to string for unsupported types
    }
}
