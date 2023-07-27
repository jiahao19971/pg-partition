
## Create

create_table_with_partitioning = "CREATE TABLE {a} ({b}) PARTITION BY RANGE ({c});"

create_partition_of_table = "CREATE TABLE {a}_{b} PARTITION OF {a} FOR VALUES FROM ('{b}-01-01 00:00:00') TO ('{c}-01-01 00:00:00');"

create_default_table_for_partition = "CREATE TABLE {a}_default PARTITION OF {a} DEFAULT;"

create_index = "CREATE INDEX idx_{a}_{b} ON {a}_{b} ({c});"

create_unique_index = "CREATE UNIQUE INDEX {a} ON {b} ({c}, {d});"

## ALTER

alter_table_constraint = "ALTER TABLE {a}_{b} ADD CONSTRAINT {a}_{b} CHECK  ({c} >= '{d}-01-01 00:00:00' AND {c} < '{e}-01-01 00:00:00');"

attach_table_partition = "ALTER TABLE {a} ATTACH PARTITION {a}_old FOR VALUES FROM ('{b}-01-01 00:00:00') TO ('{c}-01-01 00:00:00');"

attach_table_as_default_partition = "ALTER TABLE {a} ATTACH PARTITION {b} DEFAULT;"

detach_partition = "ALTER TABLE {a} DETACH PARTITION {a}_{b};"

attach_default_partition = "ALTER TABLE {a} ATTACH PARTITION {a}_default DEFAULT"

rename_table = "ALTER TABLE {a} RENAME TO {b};"

alter_table_owner = "ALTER TABLE IF EXISTS {a} OWNER TO postgres;"

alter_sequence_owner = "ALTER SEQUENCE IF EXISTS {a}_id_seq OWNER TO postgres;"

alter_sequence_owned_by = "ALTER SEQUENCE IF EXISTS {a}_id_seq OWNED BY {b}"

drop_table_constraint = "ALTER TABLE {a} DROP CONSTRAINT IF EXISTS {b};"

alter_table_index = "ALTER INDEX {a} RENAME TO {b};"

alter_replica_identity = "ALTER TABLE {a} REPLICA IDENTITY FULL;"

## INSERT

move_rows_to_another_table = """
    WITH moved_rows AS (
        DELETE FROM {a}_{b} a
        WHERE {c} >= '{d}-01-01 00:00:00' AND {c} < '{e}-01-01 00:00:00'
        RETURNING a.* 
    )
    INSERT INTO {a}_{d}
    SELECT {f} FROM moved_rows;
"""

## ANALYZE

run_analyze = "ANALYZE {a}_{b};"


## SELECT

table_check = """
                SELECT n.nspname as "Schema",
                    c.relname as "Name",
                    CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
                    pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size"
                    FROM pg_catalog.pg_class c
                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r','p','v','m','S','f','')
                        AND n.nspname <> 'pg_catalog'
                        AND n.nspname <> 'information_schema'
                        AND n.nspname !~ '^pg_toast'
                        AND c.relname = '{a}'
                        AND n.nspname = '{b}'
                    ORDER BY 1,2;
            """

table_check_like = """
    SELECT n.nspname as "Schema",
                    c.relname as "Name"
                    FROM pg_catalog.pg_class c
                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r','p','v','m','S','f','')
                        AND n.nspname <> 'pg_catalog'
                        AND n.nspname <> 'information_schema'
                        AND n.nspname !~ '^pg_toast'
                        AND c.relname like '{a}'
                        AND c.relkind = 'r'
                        AND n.nspname = '{b}'
                    ORDER BY 1,2;
"""

default_table_check = "SELECT count(*) FROM {a}_default;"

get_table_index = "select {a} from pg_indexes where tablename = '{b}' and schemaname = '{c}';"

get_table_index_like = "select {a} from pg_indexes where tablename = '{b}' and schemaname = '{c}' and {a} like '{d}';"

get_min_max_table = "SELECT min({a}), max({a}) FROM {b};"

get_order_by_limit_1 = "SELECT {a} FROM {b} ORDER BY {c} {d} LIMIT 1;"

## DROP

drop_table_index = "DROP INDEX IF EXISTS {a};"

## SET 

set_search_path = "SET search_path to '{a}';"