from common.query import (
    detach_partition,
    set_search_path,
    create_partition_of_table,
    alter_table_owner,
    alter_table_constraint,
    get_table_existence,
    move_rows_to_another_table,
    attach_table_as_default_partition,
    get_min_table
)
import os
from tunnel.tunnel import _get_tunnel
from db.db import _get_db
from sshtunnel import SSHTunnelForwarder
from common.common import PartitionCommon
from common.wrapper import get_config_n_secret, background

class YearlyPartition(PartitionCommon):
    def __init__(self) -> None:
        super().__init__()

    def perform_split_partition(self, logger, table, year, cur, table_name, colname):
        detach_old_partition = detach_partition.format(a=table['name'], b='old')
        create_table = create_partition_of_table.format(a=table['name'], b=year, c=year + 1)
        change_table_owner = alter_table_owner.format(a=table_name)
        add_constraint_table = alter_table_constraint.format(a=table['name'], b=year, c=table['partition'], d=year, e=year + 1)
        move_lines = move_rows_to_another_table.format(a=table['name'], b='old', c=table['partition'], d=year, e=year + 1, f=",".join(colname))
        attach_as_default = attach_table_as_default_partition.format(a=table['name'], b=f"{table['name']}_old")

        logger.debug("Detach old partition table")
        cur.execute(detach_old_partition)
        logger.debug(f"Create new table for partition: {year}")
        cur.execute(create_table)
        logger.debug(f"Change table partition ownership: {year}")
        cur.execute(change_table_owner)
        logger.debug(f"Add table constraint for table: {year}")
        cur.execute(add_constraint_table)
        logger.debug(f"Migrate old data to new table: {year}")
        cur.execute(move_lines)
        logger.debug("Attach table as default partition")
        cur.execute(attach_as_default)

    @background
    def yearly_partition(self, table, database_config, application_name, event):
        table     = event['table']
        year      = event['year']

        db_identifier = database_config['db_identifier']
        logger = self.logging_func(application_name=application_name)

        server = _get_tunnel(database_config)
        conn = _get_db(server, database_config, application_name)
        logger.debug(f"Connected: {db_identifier}")

        conn = conn.connect()
        cur = conn.cursor()

        _, colname = self._get_column(table)

        try:
            partitioning = self.check_table_partition(table, cur)
            
            if partitioning is False:
                table_name = f"{table['name']}_{year}"

                set_searchPath = set_search_path.format(a=table['schema'])
                logger.debug(set_searchPath)
                cur.execute(set_searchPath)

                try:
                    deployment = os.environ['DEPLOYMENT']
                    if deployment == "kubernetes":
                        ## Check default min and migrate the date for the year
                        min_table = get_min_table.format(a=table['partition'], b=f"{table['name']}_old")
                        logger.debug(min_table)
                        cur.execute(min_table)
                        min_date = cur.fetchall()

                        min_check_date = min_date[0][0].year

                        year = min_check_date
                        table_name = f"{table['name']}_{year}"

                        self.perform_split_partition(logger, table, year, cur, table_name, colname)
                    else:
                        raise Exception
                except Exception as e:
                    logger.debug("Skipping min migration checked")
                    check_information_schema = get_table_existence.format(a=table['schema'], b=table_name)
                    logger.debug(check_information_schema)
                    cur.execute(check_information_schema)

                    data = cur.fetchall()

                    if data[0][0] == 1:
                        logger.error(f"Table {table_name} already exist")
                    else:
                        self.perform_split_partition(table, year, cur, table_name, colname)

                conn.commit()
            else:
                logger.info(f"Partitioning needed for table {table['schema']}.{table['name']}")
            conn.close()    
            if type(server) == SSHTunnelForwarder:
                server.stop()
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Error occured while yearly partitioning, rolling back")
            conn.rollback()
            conn.close()

    @get_config_n_secret
    def main(self, table, database_config, application_name):
        event = {
            "table": table,
            "year": 2017,
        }
        self.yearly_partition(table, database_config, application_name, event)

if __name__ == "__main__":
    yearly = YearlyPartition()
    yearly.main()