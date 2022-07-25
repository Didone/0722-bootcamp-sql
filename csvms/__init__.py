""" CSVMS Module """
import logging
import re
from os import environ

__version__ = '0.2.14'

def logger(identifier:str=None):
    """CSVMS Logger"""
    logging.basicConfig(format="%(asctime)s: %(message)s")
    if identifier is None:
        identifier = __name__
    log = logging.getLogger(identifier)
    log.setLevel(getattr(logging, environ.get('LOG_LEVEL', 'INFO')))
    return log

logger().debug("version:%s", __version__)

def pyproject(location:str) -> dict():
    """ Parse toml file to dictionary
    :return: Dictionary with toml structure
    """
    _props = dict()
    _prop = str()
    with open(location, encoding="utf-8") as _file:
        _pyproject = _file.readlines()
    for conf in _pyproject:
        try:
            _prop = next(re.finditer(r"^\[(.+)\]$", conf, re.IGNORECASE)).group(1)
            _props[_prop]=dict()
        except StopIteration:
            try:
                matches = next(re.finditer(r"""(.+) = (\"|\[)(.+)(\"|\])""", conf, re.IGNORECASE))
                _props[_prop].update({matches.group(1).replace('\"',''):matches.group(3).replace('\"','')})
            except StopIteration:
                continue
        except Exception as err:
            raise err
    return _props
