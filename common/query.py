"""
  All the query that will be perform
  for the entire pg_partition script
"""
import logging
import os

from common.common_enum import DEBUGGER

logging.basicConfig(
  format="%(asctime)s - %(levelname)s: %(name)s @ %(message)s",
  datefmt="%Y-%m-%d %H:%M:%S",
  level=logging.WARNING,
)


class PartitionQuery:
  """
  PartitionQuery class is used to
  store all the query that will be perform
  by the pg_partition script

  Args:
    No args needed

  Returns:
    No returns
  """

  env_string = (
    "Environment variable %s was not found/have issue, "
    "switching back to default value: %s"
  )

  ## Create
  create_normal_table = """
    CREATE TABLE {a} ({b},
      primary key ({c})
    )
  """

  create_table_with_partitioning = (
    "CREATE TABLE {a} ({b}) PARTITION BY RANGE ({c});"
  )

  create_partition_of_table = """
    CREATE TABLE {a}_{b}
    PARTITION OF {a} FOR VALUES
    FROM ('{b}-01-01 00:00:00') TO ('{c}-01-01 00:00:00');
  """

  create_aws_s3_extension = "CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;"

  create_unique_index = """
    CREATE UNIQUE INDEX {a}
    ON ONLY {b}
    USING btree ({c}, {d});
  """

  create_unique_index_concurrently = """
    CREATE UNIQUE INDEX CONCURRENTLY {a}
    ON {b}
    USING btree ({c}, {d});
  """

  create_index_concurrently = """
    CREATE INDEX CONCURRENTLY {a}
    ON {b}
    {c};
  """

  create_normal_index = """
    CREATE INDEX {a}
    ON {b} {c};
  """

  create_sequece_if_not_exists = """
    CREATE SEQUENCE IF NOT EXISTS "{a}".{b}
    START WITH {c}
    INCREMENT BY {d};
  """

  set_sequence_last_val = """
    select setval('"{a}".{b}', {c}, true);
  """

  ## ALTER

  alter_table_constraint = """
    ALTER TABLE {a}_{b}
    ADD CONSTRAINT {a}_{b}
    CHECK  ({c} >= '{d}-01-01 00:00:00' AND {c} < '{e}-01-01 00:00:00');
  """

  attach_table_as_default_partition = (
    "ALTER TABLE {a} ATTACH PARTITION {b} DEFAULT;"
  )

  alter_table_add_partition = """
    ALTER TABLE {a} ATTACH PARTITION {b} FOR VALUES FROM ('{c}-01-01 00:00:00') TO ('{d}-01-01 00:00:00');
  """

  detach_partition = "ALTER TABLE {a} DETACH PARTITION {a}_{b};"

  rename_table = "ALTER TABLE {a} RENAME TO {b};"

  alter_table_owner = "ALTER TABLE IF EXISTS {a} OWNER TO postgres;"

  alter_sequence_owner = "ALTER SEQUENCE IF EXISTS {a} OWNER TO postgres;"

  alter_sequence_owned_by = "ALTER SEQUENCE IF EXISTS {a} OWNED BY {b}"

  alter_replica_identity = "ALTER TABLE {a} REPLICA IDENTITY FULL;"

  alter_column_not_null = """
    ALTER TABLE "{a}".{b}
    ALTER COLUMN {c}
    SET NOT NULL;
  """

  alter_table_drop_constraint_add_primary = """
    ALTER TABLE "{a}".{b}
    DROP CONSTRAINT {b}_pkey,
    ADD PRIMARY KEY ({c}, {d})
  """

  alter_index_rename = "ALTER INDEX {a} RENAME TO {b}"

  alter_index_attach_partition = "ALTER INDEX {a} ATTACH PARTITION {b};"

  alter_table_set_default_val = """
    ALTER TABLE "{a}".{b}
    ALTER COLUMN {c}
    SET DEFAULT nextval('"{a}".{d}'::regclass);
  """

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

  insert_data_to_table = """
    INSERT INTO {a}
    SELECT {b}
    FROM {c};
  """

  ## SELECT

  table_check = """
    SELECT n.nspname as "Schema",
      c.relname as "Name",
      CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized view'
        WHEN 'i' THEN 'index'
        WHEN 'S' THEN 'sequence'
        WHEN 's' THEN 'special'
        WHEN 'f' THEN 'foreign table'
        WHEN 'p' THEN 'partitioned table'
        WHEN 'I' THEN 'partitioned index'
        END as "Type",
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
  get_table_existence = """
    SELECT count(*)
    FROM information_schema.tables
    WHERE table_schema='{a}' and table_name='{b}';
  """

  count_table_from_db = "SELECT count(*) FROM {a}"

  get_table_index = """
    select {a}
    from pg_indexes
    where tablename = '{b}'
      and schemaname = '{c}';
  """

  get_min_table = "SELECT min({a}) FROM {b} LIMIT 1;"

  get_order_by_limit_1 = "SELECT {a} FROM {b} ORDER BY {c} {d} LIMIT 1;"

  get_blocking_query = """
    SELECT
        activity.pid,
        activity.usename,
        activity.query,
        blocking.pid AS blocking_id,
        blocking.query AS blocking_query
    FROM pg_stat_activity AS activity
    JOIN
      pg_stat_activity AS blocking
      ON blocking.pid = ANY(pg_blocking_pids(activity.pid));
  """

  aws_migrate_data = """
    SELECT *
      FROM aws_s3.query_export_to_s3(
      'SELECT * FROM "{a}".{b}',
      aws_commons.create_s3_uri(
      '{c}',
      '{d}',
      '{e}'
      ),
    options :='format csv, HEADER true, ENCODING UTF8'
    );
  """

  get_index_from_pg_class = """
    select i.relname as indexname
    from pg_class i
        join pg_index idx on idx.indexrelid = i.oid
        join pg_class t on t.oid = idx.indrelid
        join pg_namespace n ON n.oid = t.relnamespace
    where i.relkind = 'I'
        and t.relname = '{a}'
        and n.nspname = '{b}';
  """

  get_sequence_like_value = """
    SELECT sequencename, start_value, increment_by, last_value
    FROM pg_sequences
    WHERE sequencename like '{a}%'
    and schemaname = '{b}';
  """

  ## DROP

  drop_table = 'DROP TABLE "{a}".{b};'

  drop_table_cascade = "DROP TABLE {a} CASCADE;"

  ## SET

  set_search_path = "SET search_path to '{a}';"

  ## ANALYZE

  analyze_table = "ANALYZE {a};"

  def __init__(self) -> None:
    self.logger = logging.getLogger("PG_Partition")
    self.logger = self.logging_func("PG_Partition")

  def _check_logger(self) -> str:
    try:
      logger = DEBUGGER(os.environ["LOGLEVEL"])
      self.logger.debug("Environment variable LOGLEVEL was found")
      return logger.value
    except ValueError as e:
      self.logger.error(e)
      raise e
    except KeyError:
      self.logger.debug(self.env_string, "LOGLEVEL", DEBUGGER.DEBUG.value)
      return DEBUGGER.DEBUG.value

  def _evaluate_logger(self, logs):
    if logs == DEBUGGER.ERROR.value:
      return logging.ERROR
    elif logs == DEBUGGER.INFO.value:
      return logging.INFO
    elif logs == DEBUGGER.WARNING.value:
      return logging.WARNING
    else:
      return logging.DEBUG

  def logging_func(self, application_name="PG_Partition"):
    logger = logging.getLogger(application_name)
    logs = self._check_logger()
    logger.setLevel(self._evaluate_logger(logs))

    return logger
