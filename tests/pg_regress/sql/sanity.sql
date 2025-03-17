CREATE TABLE r (a int primary key, b text);
CALL mooncake.create_table('c', 'r');
INSERT INTO r VALUES (1, 'a'), (2, 'b'), (3, 'c');
UPDATE r SET b = a + 1 WHERE a > 2;
DELETE FROM r WHERE a < 2;
SELECT * FROM c;
DROP TABLE r, c;
