from dotenv import load_dotenv
import psycopg2, logging, os
from common.common import logs as loggers

load_dotenv()

class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """
    def __init__(self, appname):
        self.appname = appname
        super

    def filter(self, record):
        record.APPNAME = self.appname
        return True

class DBLoader():

    env_string = (
        "Environment variable %s was not found/have issue, "
        "switching back to default value: %s"
    )
      
    def __init__(self, server, database, application_name="DB_Loader"):
        self.logger = loggers(f"DB_{database}")
        try:
            self.logger.info(f"Database {database} Initialize: {server.local_bind_host}")
        except:
            self.logger.info(f"Database {database} Initialize: {server['local_bind_host']}")
        self.db_user=self._get_database_username()
        self.db_pass=self._get_database_password()
        self.database = database 
        self.server = server
        self.application_name = application_name

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
        except Exception as e:
            try:
                conn = psycopg2.connect(
                    database=self.database,
                    user=self.db_user,
                    host=self.server.local_bind_host,
                    port=self.server.local_bind_port,
                    password=self.db_pass,
                    application_name=self.application_name
                )
            except Exception as e:
                conn = psycopg2.connect(
                    database=self.database,
                    user=self.db_user,
                    host=self.server['local_bind_host'],
                    port=self.server['local_bind_port'],
                    password=self.db_pass,
                    application_name=self.application_name
                )

        return conn