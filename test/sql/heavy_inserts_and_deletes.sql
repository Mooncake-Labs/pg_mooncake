-- Create a temporary table for testing
CREATE TEMPORARY TABLE test_table (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- Insert a large number of rows with different strings
INSERT INTO test_table (data)
SELECT 'Test data ' || id
FROM generate_series(1, 1000000) AS id;

-- Delete a specific number of rows from the table
DELETE FROM test_table
WHERE id <= 999999;

select * from test_table;
-- Drop the temporary table
DROP TABLE test_table;