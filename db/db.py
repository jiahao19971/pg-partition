from dotenv import load_dotenv
import os
import logging
import psycopg2


load_dotenv()
logging.basicConfig(format="%(asctime)s - %(levelname)s: %(message)s")

class DBLoader():

    env_string = (
        "Environment variable %s was not found/have issue, "
        "switching back to default value: %s"
    )
      
    def __init__(self, server, database):
        self.logger = logging.getLogger("PGUpgrader")
        self.logger.info(f"Database {database} Initialize: {server['local_bind_host']}")
        self.db_user=self._get_database_username()
        self.db_pass=self._get_database_password()
        self.database = database 
        self.server = server

    def _get_database_username(self):
        try:
            username: str = os.environ['USERNAME']
            self.logger.debug("Environment variable USERNAME was found")
            return username
        except ValueError as e:
            self.logger.error("Environment variable USERNAME was not found")
            raise ValueError(e)
        
    def _get_database_password(self):
        try:
            password: str = os.environ['PASSWORD']
            self.logger.debug("Environment variable PASSWORD was found")
            return password
        except ValueError as e:
            self.logger.error("Environment variable PASSWORD was not found")
            raise ValueError(e)

    def connect(self):
        try:
            conn = psycopg2.connect(
                        database=self.database,
                        user=self.db_user,
                        host=self.server['local_bind_host'],
                        port=self.server['local_bind_port'],
                        password=self.db_pass,
                        sslmode=os.environ['DB_SSLMODE'],
                        sslrootcert=os.environ['DB_SSLROOTCERT'],
                        sslcert=os.environ['DB_SSLCERT'],
                        sslkey=os.environ['DB_SSLKEY']
                    )
        except:
            conn = psycopg2.connect(
                database=self.database,
                user=self.db_user,
                host=self.server['local_bind_host'],
                port=self.server['local_bind_port'],
                password=self.db_pass
            )
        

        return conn