"""
    Check_blocker module is used to check if there is any block query
    It utilize psycopy2 to connect to the database and execute the query
    Additional feature: it is able to perform tunneling to the database
"""
import random
import time

import timeout_decorator
from dotenv import load_dotenv

from common.common import PartitionCommon
from common.wrapper import get_config_n_secret
from db.db import get_db
from tunnel.tunnel import get_tunnel

load_dotenv()


class RandomInsert(PartitionCommon):
  """
  RandomInsert checked the database if there is any
  blocked query.

  Args:
    No args needed

  Returns:
    [list]: [blocked query]
  """

  @timeout_decorator.timeout(10, timeout_exception=StopIteration)
  def get_data(self, cur):
    random_stuff = [
      {"item_id": "100", "item_type": "BankInfo"},
      {"item_type": "Transaction", "item_id": "262031826"},
      {"item_type": "ReservationSet", "item_id": "4481120"},
      {"item_type": "FavePayment", "item_id": "44979638"},
    ]

    random_num = random.randint(0, len(random_stuff) - 1)
    cur.execute(
      f"""
        SELECT * FROM "singapore".versions
        WHERE item_id = {random_stuff[random_num]['item_id']}
        AND item_type = '{random_stuff[random_num]['item_type']}'
          ORDER BY created_at DESC;
      """
    )

    data = cur.fetchall()

    return data

  @get_config_n_secret
  def main(self, table=None, database_config=None, application_name=None):
    db_identifier = database_config["db_identifier"]
    self.logger = self.logging_func(application_name=application_name)

    server = get_tunnel(database_config)

    i = 0

    while True:
      conn = get_db(server, database_config, application_name)
      conn = conn.connect()
      self.logger.debug(f"Connected: {db_identifier}")
      cur = conn.cursor()
      self.logger.debug("Random select")

      try:
        data = self.get_data(cur)
        if len(data) == 0:
          self.logger.info("No data found in db")
      except StopIteration:
        self.logger.info("Query exceed 10 seconds")
      conn.close()
      self.logger.debug("Sleeping for 60 seconds")
      time.sleep(60)
      i += 1


if __name__ == "__main__":
  RandomInsert().main()
