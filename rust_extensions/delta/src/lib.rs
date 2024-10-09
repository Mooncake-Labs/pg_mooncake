use cxx::{CxxString, CxxVector};
use deltalake::kernel::{Action, Add, ArrayType, DataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{open_table, TableProperty};

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        fn DeltaCreateTable(
            table_name: &CxxString,
            location: &CxxString,
            column_names: &CxxVector<CxxString>,
            column_types: &CxxVector<CxxString>,
        ) -> Result<()>;

        fn DeltaAddFiles(
            table_location: &CxxString,
            file_paths: &CxxVector<CxxString>,
            file_sizes: &CxxVector<i64>,
        ) -> Result<()>;
    }
}

#[allow(non_snake_case)]
pub fn DeltaCreateTable(
    table_name: &CxxString,
    location: &CxxString,
    column_names: &CxxVector<CxxString>,
    column_types: &CxxVector<CxxString>,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let metadata = vec![(
            "creator".to_string(),
            serde_json::json!("pg_mooncake_extension"),
        )];
        let _table = CreateBuilder::new()
            .with_location(location.to_str()?)
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
pub fn DeltaAddFiles(
    table_location: &CxxString,
    file_paths: &CxxVector<CxxString>,
    file_sizes: &CxxVector<i64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let mut table: deltalake::DeltaTable = open_table(table_location.to_string()).await?;
        let mut actions = Vec::new();
        for (file_path, file_size) in file_paths.iter().zip(file_sizes.iter()) {
            let add = Add {
                path: file_path.to_string(),
                size: *file_size,
                data_change: true,
                ..Default::default()
            };
            actions.push(Action::Add(add));
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
