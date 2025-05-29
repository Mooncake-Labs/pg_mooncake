## 0.1.2 (2025-02-11)
### Added
- Support NOT NULL constraint (#116)
### Fixed
- Manully load iceberg extension since it's not autoloadable in DuckDB v1.1.3
- Fix use-after-free bug when reading statistics
- Allow non-superusers to set maximum_memory and maximum_threads
- Fix ALTER TABLE ... SET ACCESS METHOD DEFAULT (#115)

## 0.1.1 (2025-01-29)
### Added
- Preload pg_mooncake in Docker image
- Add a command to reset DuckDB
- Expose GUCs to set maximum memory and threads DuckDB can use
### Fixed
- Fix DuckDB extension autoloading (#99)
- Fix query failure involving subplans (#100)
- Suppress unnecessary default value error on Postgres heap tables

## 0.1.0 (2025-01-10)
### Added
- Transactional INSERT, SELECT, UPDATE, DELETE, and COPY
- JOIN with regular Postgres heap tables
- Load Parquet, CSV, and JSON files into columnstore tables
- Read existing Iceberg and Delta Lake tables
- File statistics and skipping
- Write Delta Lake tables
