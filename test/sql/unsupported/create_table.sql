CREATE TABLE t (a int GENERATED ALWAYS AS (b + 1) STORED, b int) USING columnstore;
