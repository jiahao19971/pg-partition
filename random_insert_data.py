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


class RandomInsert(PartitionCommon):
  """
  RandomInsert checked the database if there is any
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

    i = 0

    while True:
      conn = get_db(server, database_config, application_name)
      conn = conn.connect()
      self.logger.debug(f"Connected: {db_identifier}")
      cur = conn.cursor()
      current_time = time.strftime("%H:%M:%S", time.localtime())
      cur = conn.cursor()
      self.logger.debug("Inserting data to db")
      cur.execute(
        f"""
        INSERT INTO "singapore".versions
          (item_type, item_id, event, object, created_at, object_changes)
        VALUES (
          'BankInfo',
          100,
          'update',
          '---
            id: 40
            name: Intesa Sanpaolo S.P.A. SG
            swift_code: BCITSGSG
            created_at: 2017-11-20 02:41:28.702076000 Z
            account_number_length:
            bank_code_string:
            bank_code: 8350
            ',
            '2023-11-25 {current_time}'
            ,
            '---
            bank_code_string:
            -
            - "8350"
            ');
      """
      )
      conn.commit()
      conn.close()
      self.logger.debug("Sleeping for 10 seconds")
      time.sleep(10)

      i += 1


if __name__ == "__main__":
  RandomInsert().main()
