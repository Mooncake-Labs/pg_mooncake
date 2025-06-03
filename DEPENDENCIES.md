# Dependencies

### moonlink
Moonlink is a Rust library that provides real-time synchronization between Postgres and Iceberg. It is a key component enabling pg_mooncake columnstore to support point inserts, updates, and deletes, while maintaining consistency with the base OLTP table.

- **Version**: v0.1.0
- **License**: [Business Source License (BSL)](https://mariadb.com/bsl-faq-adopting/)
- **License Details**: moonlink is licensed under the Business Source License with a specific exception clause for pg_mooncake, allowing pg_mooncake to use moonlink without the restrictions typically imposed by the BSL.
- **Purpose**: moonlink provides essential functionality for pg_mooncake v0.2.0 and later versions.

## License Compliance

pg_mooncake itself is licensed under the MIT License, which allows for use, modification, and distribution with minimal restrictions. The dependencies listed above maintain their own licenses, and users of pg_mooncake should be aware of and comply with these licenses when applicable.

The exception clause in moonlink's BSL license specifically permits pg_mooncake to use moonlink without the usual BSL restrictions, ensuring that pg_mooncake can maintain its MIT license while depending on moonlink.

---