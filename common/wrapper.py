from tunnel.tunnel import _get_tunnel
from sshtunnel import SSHTunnelForwarder
from db.db import _get_db

def starter(func):
    def wrapped(*args):
        self = args[0]
        
        config = self._get_config()
        for table in config['table']:
            try:
                secret = self._get_secret()

                db = secret['database'] 

                for database_config in db:
                    try:
                        db_identifier = database_config['db_identifier']

                        server = _get_tunnel(database_config)
                        application_name = f"{db_identifier}:{table['schema']}.{table['name']}"
                        logger = self.logging_func(application_name=application_name)

                        conn = _get_db(server, database_config, application_name)
                        logger.debug(f"Connected: {db_identifier}")

                        func(
                            *args, 
                            conn,
                            table, 
                            logger,
                            database_config,
                            server,
                            application_name
                        )

                        if type(server) == SSHTunnelForwarder:
                            server.stop()
                    except Exception as e:
                        self.logger.error(e)
            except Exception as e:
                self.logger.error(e)
    return wrapped