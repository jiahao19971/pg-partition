from tunnel.tunnel import _get_tunnel
from sshtunnel import SSHTunnelForwarder
from db.db import _get_db
import asyncio

def background(f):
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


def get_config_n_secret(func):
    def wrapped(*args, **kwargs):
        self = args[0]
        
        secret = self._get_secret()
        db = secret['database'] 

        for database_config in db:
            try:
                config = self._get_config()
                for table in config['table']:
                    try:
                        db_identifier = database_config['db_identifier']
                        
                        application_name = f"{db_identifier}:{table['schema']}.{table['name']}"

                        func(*args,
                            table,
                            database_config,
                            application_name
                        )
                    except Exception as e:
                        self.logger.error(e)
            except Exception as e:
                self.logger.error(e)
    return wrapped