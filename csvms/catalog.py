"""Catalog Module"""
import json
from os import makedirs
from os import environ

# Local module
from csvms import logger
from csvms.exceptions import TableException

log = logger()

class Catalog():
    """Represents objects manage by the system"""
    CATALOG_FILE = environ.get('CSVMS_CATALOG', 'catalog.json')

    def __init__(self, directory:str) -> "Catalog":
        self.location = f"{directory}/{Catalog.CATALOG_FILE}"
        self.objects = dict() # List of all objects in the catalog
        try:
            with open(file=self.location, mode="r", encoding="utf-8") as infile:
                self.objects = json.load(infile)
        except FileNotFoundError:
            log.info("creating new data dictionary in %s", self.location)
            makedirs(directory, exist_ok=True)
            self.objects = dict()
            self._save_()

    def _save_(self) -> bool:
        """Save data dictionary on disk"""
        with open(file=self.location, mode="w", encoding="utf-8") as outfile:
            json.dump(self.objects, outfile, indent=2)

    def __getitem__(self, key):
        """Get table definition from catalog"""
        try:
            return self.objects[key]
        except KeyError as err:
            log.debug("Table %s not found on catalog: %s",key, err)
            raise TableException(f"Table {key} not found")

    def __setitem__(self, key:str, value:dict) -> bool:
        """Update catalog"""
        self.objects[key] = value
        self._save_()

    def __delitem__(self, key:str) -> bool:
        """Remove table from catalog"""
        del self.objects[key]
        self._save_()
