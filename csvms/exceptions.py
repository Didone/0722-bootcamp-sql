"""Exception Module"""
from csvms import logger
log = logger()

class DefaultException(Exception):
    """Base class for exceptions"""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        log.debug(*args)

class TableException(DefaultException):
    """Base class for Table exceptions"""

class ColumnException(DefaultException):
    """Base class for Column exceptions"""

class DataException(DefaultException):
    """Base class for Data exceptions"""
