"""
This is the main enum for TunnelHandler
Consist of all static variable in the tunnel
"""
from enum import Enum


class DEBUGGER(Enum):
  """
  This enum consist of the log env parameter

  It is used to validate if the log provided is
  fall into the list below
  """

  DEBUG = "DEBUG"
  INFO = "INFO"
  WARNING = "WARNING"
  ERROR = "ERROR"

  @classmethod
  def _missing_(cls, value):
    choices = list(cls.__members__.keys())
    raise ValueError(
      f"{value} is not a valid {cls.__name__}, " f"please choose from {choices}"
    )
