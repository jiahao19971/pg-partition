"""
    CheckBlocker checked the database if there is any 
    blocked query.

    Args:
      No args needed

    Returns:
      [dict]: [blocked query]
""" 
import time
from dotenv import load_dotenv
from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
from db.db import _get_db
from tunnel.tunnel import _get_tunnel

load_dotenv()


class CheckBlocker(PartitionCommon):
  """
    CheckBlocker checked the database if there is any 
    blocked query.

    Args:
      No args needed

    Returns:
      [dict]: [blocked query]
  """ 
  @get_config_n_secret
  def main(self, table, database_config, application_name):
    db_identifier = database_config["db_identifier"]
    logger = self.logging_func(application_name=application_name)

    server = _get_tunnel(database_config)
    conn = _get_db(server, database_config, application_name)
    logger.debug(f"Connected: {db_identifier}")

    conn = conn.connect()
    cur = conn.cursor()

    query = """
            SELECT
                activity.pid,
                activity.usename,
                activity.query,
                blocking.pid AS blocking_id,
                blocking.query AS blocking_query
            FROM pg_stat_activity AS activity
            JOIN pg_stat_activity AS blocking ON blocking.pid = ANY(pg_blocking_pids(activity.pid));
        """

    while True:
      cur = conn.cursor()
      cur.execute(query)
      blocker = cur.fetchall()
      print(blocker)
      conn.commit()
      time.sleep(60)


if __name__ == "__main__":
  CheckBlocker().main()
