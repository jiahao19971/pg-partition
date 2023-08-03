from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import os, io
import paramiko
import socket
from common.common import PartitionCommon

load_dotenv()

class Tunneler(PartitionCommon):
    def __init__(
        self, 
        instance: str, 
        instance_port: int,
        remote_host: str=False,
        remote_port: str=False,
        remote_username: str=False,
        remote_key: str=False,
        remote_password: str=False
    ):
        super().__init__()
        self.logger = self.logging_func("DB_Tunneler")
        logs = self._check_logger()
        self.logger.setLevel(self._evaluate_logger(logs))
        self.logger.info(f"Tunnel Initialize: {instance}")
        self.remote_host=self._get_remote_host(remote_host)
        self.remote_port=self._get_remote_port(remote_port)
        self.remote_username=self._get_remote_username(remote_username)
        self.remote_auth, self.remote_auth_type=self._get_pass_or_key(remote_key, remote_password)
        self.instance=instance
        self.instance_port=instance_port

    def _get_remote_host(self, remote_host):
        try:
            if remote_host is not False:
                host: str = remote_host
            else:
                host: str = os.environ['REMOTE_HOST']
            self.logger.debug("Environment variable REMOTE_HOST was found")
            return host
        except ValueError as e:
            self.logger.error("Environment variable REMOTE_HOST was not found")
            raise ValueError(e)
        
    def _get_remote_username(self, remote_username):
        try:
            if remote_username is not False:
                username: str = remote_username
            else:
                username: str = os.environ['REMOTE_USERNAME']
            self.logger.debug("Environment variable REMOTE_USERNAME was found")
            return username
        except ValueError as e:
            self.logger.error("Environment variable REMOTE_USERNAME was not found")
            raise ValueError(e)
        
    def _get_remote_port(self, remote_port):
        try:
            if remote_port is not False:
                port: str = remote_port
            else:
                port: str = os.environ['REMOTE_PORT']
            self.logger.debug("Environment variable REMOTE_PORT was found")
            return int(port)
        except ValueError as e:
            self.logger.error("Environment variable REMOTE_PORT was not found")
            raise ValueError(e)
    
    def _get_pass_or_key(self, remote_key, remote_password):
        try:
            if remote_password is not False:
                password: str = remote_password
            else:
                password: str = os.environ['REMOTE_PASSWORD']
            return password, False
        except KeyError:
            self.logger.debug("Password does not exist, checking for key")
            try:
                if remote_key is not False:
                    key: str = remote_key
                else:
                    key: str = os.environ['REMOTE_KEY']
                with open(key, "r") as keys:
                    ssh_key=keys.read()
                    pkey = paramiko.RSAKey.from_private_key(io.StringIO(ssh_key))
                    return pkey, True
            except FileNotFoundError:
                self.logger.error("No Password or Key exist")
                raise FileNotFoundError("No Password or Key exist")

    def connect(self):
        sock = socket.socket()
        sock.bind(('', 0))
        if self.remote_auth_type:
            self.logger.info("Authenticate with Private Key...")
            server = SSHTunnelForwarder(
                (self.remote_host, self.remote_port), 
                ssh_username=self.remote_username, 
                ssh_pkey=self.remote_auth, 
                host_pkey_directories="./",
                allow_agent=False,
                remote_bind_address=(self.instance, self.instance_port), 
                local_bind_address=('localhost', sock.getsockname()[1])
            )
        else:
            self.logger.info("Authenticate with Password...")
            server = SSHTunnelForwarder(
                (self.remote_host, self.remote_port), 
                ssh_username=self.remote_username, 
                ssh_password=self.remote_auth,
                remote_bind_address=(self.instance, self.instance_port), 
                local_bind_address=('localhost', sock.getsockname()[1])
            )

        return server


def _get_tunnel(database_config):
    DB_PORT=5432
    REMOTE_HOST=False
    REMOTE_PORT=False
    REMOTE_USERNAME=False
    REMOTE_KEY=False
    REMOTE_PASSWORD=False
    if "db_host" in database_config:
        DB_HOST=database_config['db_host']
    else:
        DB_HOST=os.environ['DB_HOST']
    if "db_port" in database_config:
        DB_PORT=int(database_config['db_port'])

    if "remote_host" in database_config:
        REMOTE_HOST=database_config["remote_host"]
        REMOTE_PORT=database_config["remote_port"]
        REMOTE_USERNAME=database_config["remote_username"]

    if "remote_key" in database_config:
        REMOTE_KEY=database_config["remote_key"]
    
    if "remote_password" in database_config:
        REMOTE_PASSWORD=database_config["remote_password"]

    local = {
            'local_bind_host': DB_HOST,
            'local_bind_port': DB_PORT,
        }
    if REMOTE_HOST is False:
        server = local
    else:
        try:
            server = Tunneler(
                DB_HOST, 
                DB_PORT,
                REMOTE_HOST,
                REMOTE_PORT,
                REMOTE_USERNAME,
                REMOTE_KEY,
                REMOTE_PASSWORD
            )

            server = server.connect()

            server.start()
        except:
            server = local

    return server