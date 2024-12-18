CREATE TABLE t (a int, b text);
CREATE TABLE t_log (operation text, a int, b text) USING COLUMNSTORE;

CREATE FUNCTION t_trigger_handler()
RETURNS TRIGGER LANGUAGE 'plpgsql'
AS
$func$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO t_log VALUES ('INSERT', NEW.a, NEW.b);
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO t_log VALUES ('DELETE', OLD.a, OLD.b);
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO t_log VALUES ('DELETE', OLD.a, OLD.b), ('INSERT', NEW.a, NEW.b);
    END IF;
    RETURN NEW;
END;
$func$;

CREATE TRIGGER t_trigger
AFTER INSERT OR DELETE OR UPDATE ON t
FOR EACH ROW
EXECUTE FUNCTION t_trigger_handler();

INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');
INSERT INTO t VALUES (2, 'f'), (3, 'g'), (4, 'h');
UPDATE t SET b = a + 1 WHERE a > 3;
DELETE FROM t WHERE a < 3;

SELECT * FROM t;
SELECT * FROM t_log;

DROP TABLE t, t_log;
DROP FUNCTION t_trigger_handler;
