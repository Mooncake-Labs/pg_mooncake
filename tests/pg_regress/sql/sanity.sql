CREATE TABLE r (a int PRIMARY KEY, b text);
CALL mooncake.create_table('c', 'r');
INSERT INTO r VALUES (1, 'a'), (2, 'b'), (3, 'c');
UPDATE r SET b = a + 1 WHERE a > 2;
DELETE FROM r WHERE a < 2;
SELECT * FROM c ORDER BY a;
-- create_table reports why creating the mirror table failed.
CALL mooncake.create_table('c2', 'no_such_table');
CREATE TABLE taken (a int);
CALL mooncake.create_table('taken', 'r');
DROP TABLE taken;
DROP TABLE r, c;
