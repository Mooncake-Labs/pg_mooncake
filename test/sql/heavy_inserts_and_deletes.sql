-- Create a temporary table for testing
CREATE TABLE test_table (
    id INTEGER,
    data TEXT
) using columnstore;

-- Insert a large number of rows with different strings
INSERT INTO test_table (id, data)
SELECT id, 'Test data ' || id
FROM generate_series(1, 100000) AS id;

-- Simple select
select count(*) from test_table;
select * from test_table limit 10;

-- Update a specific range of rows
UPDATE test_table
SET data = 'Updated data ' || id
WHERE id <= 50 and id >= 60;

select * from test_table where id < 100;

-- Delete a specific number of rows from the table
DELETE FROM test_table
WHERE id <= 999999;

select * from test_table;
-- Drop the temporary table
DROP TABLE test_table;