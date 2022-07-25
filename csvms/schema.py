""" Schema Module """
from pathlib import Path
from os import makedirs
from os import environ
# Local module
from csvms import logger
from csvms.catalog import Catalog

log = logger()

class Database():
    """The database is an file system directory
       Used to store table data files on the local file system"""
    DEFAULT_DB = environ.get('CSVMS_DEFAULT_DB', "default")
    FILE_DIR = environ.get('CSVMS_FILE_DIR', 'data')

    def __init__(self, name:str=None, temp=False) -> "Database":
        """Database representation
        :param name: Database identifier. If 'None' use default name. Default is None
        :param temp: If false, create directory. Default False
        """
        self.catalog = Catalog(Database.FILE_DIR)
        self.name = name
        if not isinstance(name,str):
            self.name = Database.DEFAULT_DB
        self.location = str()
        if not temp:
            self.location = Database.create_location(self.name)

    @classmethod
    def create_location(cls, location:str) -> str:
        """Create directory
        :param location: Filesystem path. Default directory is "database"
        :return: Location path
        """
        _path = Path(Database.FILE_DIR).joinpath(location)
        try:
            makedirs(_path, exist_ok=True)
        except OSError as err:
            log.debug(err)
        return _path
