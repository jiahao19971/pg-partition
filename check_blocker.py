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

    n = 0
    while True:
      conn = get_db(server, database_config, application_name)
      conn = conn.connect()
      self.logger.debug(f"Connected: {db_identifier}")
      cur = conn.cursor()
      cur.execute(self.get_blocking_query)
      blocker = cur.fetchall()
      if len(blocker) > 0:
        n += 1
        self.logger.info(f"Blocker found: {n}")
        self.logger.info(blocker)
        self.logger.info("Found blocking query")

      conn.commit()
      conn.close()
      self.logger.debug("Sleeping for 5 seconds")
      time.sleep(5)


if __name__ == "__main__":
  CheckBlocker().main()
