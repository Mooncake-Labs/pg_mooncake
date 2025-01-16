-- Create a temporary table for testing
CREATE TABLE test_table (
    id INTEGER,
    data TEXT
);

-- Insert a large number of rows with different strings
INSERT INTO test_table (id, data)
SELECT id, 'Test data ' || id
FROM generate_series(1, 1000000) AS id;

-- Update a specific range of rows
UPDATE test_table
SET data = 'Updated data ' || id
WHERE id BETWEEN 500000 AND 500010;

select * from test_table where id between 500000 and 500010;

-- Delete a specific number of rows from the table
DELETE FROM test_table
WHERE id <= 999999;

select * from test_table;
-- Drop the temporary table
DROP TABLE test_table;