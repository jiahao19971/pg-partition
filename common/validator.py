"""This is an addon module to for cerberus validator

This class component is used to validate when autoscaledown is True,
operate_day component should be there.
"""

from cerberus import Validator, errors

FIELD_TOGETHER_ERROR = 'Both field must not exist together'
FILED_EXIST_ERROR = 'At least one field must exist'

class PartitioningValidator(Validator):
  def _check_with_operation(self, field, value):
    if field == "remote_key" and value:
      if "remote_password" in self.document:
        self._error("remote_password", errors.REQUIRED_FIELD, "check_with")
    elif field == "remote_password" and value:
      if "remote_key" in self.document:
        self._error("remote_key", errors.REQUIRED_FIELD, "check_with")
    elif field == "remote_host" and value:
      if "remote_key" in self.document and "remote_password" in self.document:
        self._error("remote_key", FIELD_TOGETHER_ERROR)
        self._error("remote_password", FIELD_TOGETHER_ERROR)
      elif "remote_key" not in self.document and "remote_password" not in self.document:
        self._error("remote_key", FILED_EXIST_ERROR)
        self._error("remote_password", FILED_EXIST_ERROR)
    elif field == "db_password" and value:
      if "db_ssl" not in self.document:  
        self._error("db_password", errors.REQUIRED_FIELD, "check_with")