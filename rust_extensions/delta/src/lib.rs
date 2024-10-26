use cxx::{CxxString, CxxVector};
use deltalake::aws::register_handlers;
use deltalake::kernel::{Action, Add, ArrayType, DataType, PrimitiveType, Remove, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{open_table_with_storage_options, TableProperty};
use std::collections::HashMap;

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn DeltaInit();

        fn DeltaCreateTable(
            table_name: &CxxString,
            path: &CxxString,
            options: &CxxString,
            column_names: &CxxVector<CxxString>,
            column_types: &CxxVector<CxxString>,
        ) -> Result<()>;

        fn DeltaModifyFiles(
            path: &CxxString,
            options: &CxxString,
            file_paths: &CxxVector<CxxString>,
            file_sizes: &CxxVector<i64>,
            is_add_files: &CxxVector<i8>,
        ) -> Result<()>;
    }
}

#[allow(non_snake_case)]
pub fn DeltaInit() {
    // Register S3 handlers
    register_handlers(None);
}

#[allow(non_snake_case)]
pub fn DeltaCreateTable(
    table_name: &CxxString,
    path: &CxxString,
    options: &CxxString,
    column_names: &CxxVector<CxxString>,
    column_types: &CxxVector<CxxString>,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let mut storage_options: HashMap<String, String> =
            serde_json::from_str(options.to_str()?).expect("invalid options");
        // Write directly to S3 without locking is safe since Mooncake is the only writer
        storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
        let metadata = vec![(
            "creator".to_string(),
            serde_json::json!("pg_mooncake_extension"),
        )];
        let _table = CreateBuilder::new()
            .with_location(path.to_str()?)
            .with_storage_options(storage_options)
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
    path: &CxxString,
    options: &CxxString,
    file_paths: &CxxVector<CxxString>,
    file_sizes: &CxxVector<i64>,
    is_add_files: &CxxVector<i8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
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
        let mut storage_options: HashMap<String, String> =
            serde_json::from_str(options.to_str()?).expect("invalid options");
        // Write directly to S3 without locking is safe since Mooncake is the only writer
        storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
        let mut table: deltalake::DeltaTable =
            open_table_with_storage_options(path.to_string(), storage_options).await?;
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

fn map_postgres_columns(
    column_names: &CxxVector<CxxString>,
    column_types: &CxxVector<CxxString>,
) -> Vec<StructField> {
    column_names
        .into_iter()
        .zip(column_types.into_iter())
        .map(|(column_name, column_type)| {
            StructField::new(
                column_name.to_string(),
                convert_postgres_to_delta_type(&column_type.to_string()),
                true, // Assuming all columns are nullable for simplicity
            )
        })
        .collect()
}

fn convert_postgres_to_delta_type(column_type: &str) -> DataType {
    match column_type {
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
        _ if column_type.ends_with("[]") => {
            let base_type = &column_type[..column_type.len() - 2];
            DataType::from(ArrayType::new(
                convert_postgres_to_delta_type(base_type),
                true,
            ))
        }
        _ => DataType::Primitive(PrimitiveType::String), // Default to string for unsupported types
    }
}
