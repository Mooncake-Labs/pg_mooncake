\getenv pwd PWD
\set csv_file '\'' :pwd '/results/copy.csv'  '\''

COPY (SELECT * FROM generate_series(1, 5)) TO :csv_file;

CREATE TABLE t (a INT) USING columnstore;
COPY t FROM :csv_file;
SELECT * FROM t;

COPY t TO :csv_file;
COPY (SELECT * FROM t) TO :csv_file;

DROP TABLE t;
