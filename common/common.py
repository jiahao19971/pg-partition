import sys, json, logging, asyncio
from cerberus import Validator
from ruamel.yaml import YAML, YAMLError
from functools import lru_cache

yaml = YAML()


yaml.preserve_quotes = True

logging.basicConfig(format="%(asctime)s - %(levelname)s: %(APPNAME)s @ %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

def background(f):
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped

def logs(application_name="PG_Partition"):
  logs = logging.LoggerAdapter(logging.getLogger("PGPartition"), {'APPNAME': application_name})

  return logs

logger = logs("PG_Partition")

def print_psycopg2_exception(err):
    err_type, err_obj, traceback = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno

    logger.error("\npsycopg2 ERROR:", err, "on line number:", line_num)
    logger.error("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    logger.error("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    logger.error("pgerror:", err.pgerror)
    logger.error("pgcode:", err.pgcode, "\n")

@lru_cache
def _open_config(config_name):
    with open(config_name, "r", encoding="utf-8") as stream:
      try:
        data = yaml.load(stream)
        with open("config.json", "r", encoding="utf-8") as validation_rules:
          schema = json.load(validation_rules)
          v = Validator(schema)
          if v.validate(data, schema):
            logger.debug("Validated config.yml and no issue has been found")
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