CREATE FUNCTION columnstore_handler(internal) RETURNS table_am_handler
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE ACCESS METHOD columnstore TYPE TABLE HANDLER columnstore_handler;
