# pg_mooncake Tests

This directory contains tests for the pg_mooncake extension.

## Test Structure

- `pg_regress/` - SQL regression tests using the pgrx testing framework
- `stress_test.py` - Python stress test for concurrent operations and data consistency

## Running Tests

### SQL Regression Tests
```bash
make test
```

### Python Stress Test

The stress test creates a regular PostgreSQL table and a corresponding columnstore table, then runs concurrent operations to verify data consistency and correctness.

#### Prerequisites
```bash
pip install psycopg2-binary
```

#### Basic Usage
First, start pg_mooncake in development mode:
```bash
make run
```

Then in another terminal, run the stress test:
```bash
cd tests
python3 stress_test.py --duration 60 --verbose
```

#### Advanced Usage
```bash
python3 stress_test.py \
    --host /home/ubuntu/.pgrx \
    --port 28817 \
    --database pg_mooncake \
    --user ubuntu \
    --duration 120 \
    --batch-workers 3 \
    --point-workers 4 \
    --query-workers 2 \
    --verbose
```

#### Parameters
- `--host`: PostgreSQL host (default: /tmp for Unix socket)
- `--port`: PostgreSQL port (default: 28813 for pgrx development)
- `--database`: Database name (default: pg_mooncake)
- `--duration`: Test duration in seconds (default: 30)
- `--batch-workers`: Number of threads doing batch inserts (default: 2)
- `--point-workers`: Number of threads doing point operations (insert/update/delete) (default: 3)
- `--query-workers`: Number of threads querying the columnstore (default: 2)
- `--verbose`: Enable detailed logging

#### What the Stress Test Does

1. **Setup**: Creates a regular table and columnstore table using `mooncake.create_table`
2. **Concurrent Operations**:
   - Batch insert workers: Insert 100-1000 rows at once
   - Point operation workers: Single row inserts, updates, deletes
   - Query workers: Run various queries on the columnstore table
   - Consistency checker: Periodically compares row counts between tables
3. **Verification**: Checks for data consistency and reports any inconsistencies
4. **Cleanup**: Drops test tables when finished

#### Expected Output
```
==================================================
STRESS TEST RESULTS
==================================================
Batch inserts: 2500
Point inserts: 450
Updates: 123
Deletes: 67
Queries: 890
Consistency checks: 30
Inconsistencies found: 0
Errors: 0

✅ STRESS TEST PASSED - No inconsistencies or errors found
==================================================
```

The test focuses on correctness and stress testing rather than performance metrics. It verifies that:
- Concurrent writes to the regular table work correctly
- The columnstore table reflects changes from the regular table
- No data corruption or inconsistencies occur under concurrent load
- All operations complete without errors
