from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import os, io
import paramiko, logging
import socket
from .tunnelEnum import DEBUGGER
from common.common import logs as loggers


load_dotenv()

class Tunneler():

    env_string = (
        "Environment variable %s was not found/have issue, "
        "switching back to default value: %s"
    )
      
    def __init__(self, instance: str, instance_port: int):
        self.logger = loggers("DB_Tunneler")
        logs = self._check_logger()
        self.logger.setLevel(self._evaluate_logger(logs))
        self.logger.info(f"Tunnel Initialize: {instance}")
        self.remote_host=self._get_remote_host()
        self.remote_port=self._get_remote_port()
        self.remote_username=self._get_remote_username()
        self.remote_auth, self.remote_auth_type=self._get_pass_or_key()
        self.instance=instance
        self.instance_port=instance_port

    def _check_logger(self) -> str:
        try:
            logger = DEBUGGER(os.environ["LOGLEVEL"])
            self.logger.info("Environment variable LOGLEVEL was found")
            return logger.value
        except ValueError as e:
            self.logger.error(e)
            raise e
        except KeyError:
            self.logger.warning(self.env_string, "LOGLEVEL", DEBUGGER.DEBUG.value)
            return DEBUGGER.DEBUG.value

    def _evaluate_logger(self, logs):
        if logs == DEBUGGER.ERROR.value:
            return logging.ERROR
        elif logs == DEBUGGER.INFO.value:
            return logging.INFO
        elif logs == DEBUGGER.WARNING.value:
            return logging.WARNING
        else:
            return logging.DEBUG

    def _get_remote_host(self):
        try:
            host: str = os.environ['REMOTE_HOST']
            self.logger.debug("Environment variable REMOTE_HOST was found")
            return host
        except ValueError as e:
            self.logger.error("Environment variable REMOTE_HOST was not found")
            raise ValueError(e)
        
    def _get_remote_username(self):
        try:
            username: str = os.environ['REMOTE_USERNAME']
            self.logger.debug("Environment variable REMOTE_USERNAME was found")
            return username
        except ValueError as e:
            self.logger.error("Environment variable REMOTE_USERNAME was not found")
            raise ValueError(e)
        
    def _get_remote_port(self):
        try:
            port: str = os.environ['REMOTE_PORT']
            self.logger.debug("Environment variable REMOTE_PORT was found")
            return int(port)
        except ValueError as e:
            self.logger.error("Environment variable REMOTE_PORT was not found")
            raise ValueError(e)
    
    def _get_pass_or_key(self):
        try:
            password: str = os.environ['REMOTE_PASSWORD']
            return password, False
        except KeyError:
            self.logger.debug("Password does not exist, checking for key")
            try:
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

    