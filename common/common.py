import sys, json, logging, os
from cerberus import Validator
from ruamel.yaml import YAML, YAMLError
from functools import lru_cache
from common.validator import PartitioningValidator
from common.query import table_check
from common.commonEnum import DEBUGGER

yaml = YAML()

yaml.preserve_quotes = True

logging.basicConfig(
   format="%(asctime)s - %(levelname)s: %(APPNAME)s @ %(message)s", 
   datefmt='%Y-%m-%d %H:%M:%S'
)

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

class PartitionCommon:
    env_string = (
        "Environment variable %s was not found/have issue, "
        "switching back to default value: %s"
    )
      
    def __init__(self) -> None:
        self.logger = self.logging_func("PG_Partition")

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

    def logging_func(self, application_name="PG_Partition"):
        logs = logging.LoggerAdapter(logging.getLogger("PGPartition"), {'APPNAME': application_name})

        return logs
    
    def check_table_partition(self, table, cur):
        checker = table_check.format(a=table['name'], b=table['schema'])
        cur.execute(checker)
        data = cur.fetchall()

        if "partitioned table" in list(data[0]):
            return True
        else:
            return False

    def print_psycopg2_exception(self, err):
        err_type, err_obj, traceback = sys.exc_info()

        # get the line number when exception occured
        line_num = traceback.tb_lineno

        self.logger.error("\npsycopg2 ERROR:", err, "on line number:", line_num)
        self.logger.error("psycopg2 traceback:", traceback, "-- type:", err_type)

        # psycopg2 extensions.Diagnostics object attribute
        self.logger.error("\nextensions.Diagnostics:", err.diag)

        # print the pgcode and pgerror exceptions
        self.logger.error("pgerror:", err.pgerror)
        self.logger.error("pgcode:", err.pgcode, "\n")

    @lru_cache
    def _open_config(self, config_name="config.yaml"):
        with open(config_name, "r", encoding="utf-8") as stream:
            try:
                data = yaml.load(stream)
                with open("config.json", "r", encoding="utf-8") as validation_rules:
                    schema = json.load(validation_rules)
                    v = Validator(schema)
                    if v.validate(data, schema):
                        self.logger.debug("Validated config.yml and no issue has been found")
                        return data
                    else:
                        raise ValueError(v.errors)
            except ValueError as e:
                raise e
            except YAMLError as yamlerr:
                if hasattr(yamlerr, "problem_mark"):
                    pm = yamlerr.problem_mark
                    message = "Your file {} has an issue on line {} at position {}"
                    format_message = message.format(pm.name, pm.line, pm.column)
                    raise ValueError(format_message) from yamlerr
                else:
                    message = "Something went wrong while parsing config.yaml file"
                raise ValueError(message) from yamlerr
        
    @lru_cache
    def _open_secret(self, secret_name="secret.yaml"):
        with open(secret_name, "r", encoding="utf-8") as stream:
            try:
                data = yaml.load(stream)
                with open("secret.json", "r", encoding="utf-8") as validation_rules:
                    schema = json.load(validation_rules)
                    v = PartitioningValidator(schema)
                    if v.validate(data, schema):
                        self.logger.debug("Validated secret.yml and no issue has been found")
                        return data
                    else:
                        raise ValueError(v.errors)
            except ValueError as e:
                raise e
            except YAMLError as yamlerr:
                if hasattr(yamlerr, "problem_mark"):
                    pm = yamlerr.problem_mark
                    message = "Your file {} has an issue on line {} at position {}"
                    format_message = message.format(pm.name, pm.line, pm.column)
                    raise ValueError(format_message) from yamlerr
                else:
                    message = "Something went wrong while parsing config.yaml file"
                    raise ValueError(message) from yamlerr
        
    def checker_table(self, table, cur):
        application_name = f"{table['schema']}.{table['name']}"
        logger = self.logging_func(application_name=application_name)
        checker = table_check.format(a=table['name'], b=table['schema'])
        
        logger.info(f"Checking table if it is partition: {table['schema']}.{table['name']}")
        cur.execute(checker)
        data = cur.fetchall()

        return data

    def check_table_partition(self, table, cur):
        data = self.checker_table(table, cur)

        if "partitioned table" in list(data[0]):
            return False
        else:
            return True

    def _get_config(self):
        if os.environ['ENV'] == "staging":
            configfile = "config.staging.yaml"
        else:
            configfile = "config.yaml"
        config = self._open_config(configfile)

        return config

    def _get_secret(self):
        if os.environ['ENV'] == "staging":
            secret_file = "secret.staging.yaml"
        else:
            secret_file = "secret.yaml"
        secret = self._open_secret(secret_file)

        return secret

    def _get_column(self, table):
        collist = []
        colname = []

        for columnname in list(table['column'].keys()):
            newval = f"{columnname} {table['column'][columnname]}"
            collist.append(newval)
            colname.append(columnname)

        return collist, colname
