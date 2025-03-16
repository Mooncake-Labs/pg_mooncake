<div align="center">

# pg_mooncake 🥮
Postgres extension for 1000x faster analytics

[![License](https://img.shields.io/badge/License-MIT-blue)](https://github.com/Mooncake-Labs/pg_mooncake/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/Mooncake%20Slack-purple?logo=slack)](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)
[![Twitter](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=%40mooncakelabs)](https://x.com/mooncakelabs)
[![Docs](https://img.shields.io/badge/Documentation-pgmooncake.com-blue?style=flat&logo=readthedocs&logoColor=white)](https://pgmooncake.com/docs)

</div>

## Overview
**pg_mooncake** is a Postgres extension that adds columnar storage and vectorized execution (DuckDB) for fast analytics within Postgres. Postgres + pg_mooncake ranks among the top 10 fastest in [ClickBench](https://www.mooncake.dev/blog/clickbench-v0.1).

Columnstore tables are stored as [Iceberg](https://github.com/apache/iceberg) or [Delta Lake](https://github.com/delta-io/delta) tables in local file system or cloud storage.

The extension is maintained by [Mooncake Labs](https://mooncake.dev/) and is available on [Neon Postgres](https://neon.tech/home).
<div align="center">
  <a href="https://www.mooncake.dev/blog/how-we-built-pgmooncake">
    <img src="https://www.mooncake.dev/images/blog/blog_4venn.jpg" width="50%"/>
  </a>
</div>
