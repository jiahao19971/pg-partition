"""
    Check_blocker module is used to check if there is any block query
    It utilize psycopy2 to connect to the database and execute the query
    Additional feature: it is able to perform tunneling to the database
"""
import time

from dotenv import load_dotenv

from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
from db.db import get_db
from tunnel.tunnel import get_tunnel

load_dotenv()


class CheckBlocker(PartitionCommon):
  """
  CheckBlocker checked the database if there is any
  blocked query.

  Args:
    No args needed

  Returns:
    [list]: [blocked query]
  """

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    db_identifier = database_config["db_identifier"]
    self.logger = self.logging_func(application_name=application_name)

    server = get_tunnel(database_config)
    conn = get_db(server, database_config, application_name)

    conn = conn.connect()
    self.logger.debug(f"Connected: {db_identifier}")
    cur = conn.cursor()

    while True:
      cur = conn.cursor()
      cur.execute(self.get_blocking_query)
      blocker = cur.fetchall()
      self.logger.debug(blocker)
      conn.commit()
      self.logger.debug("Sleeping for 60 seconds")
      time.sleep(60)


if __name__ == "__main__":
  CheckBlocker().main()
