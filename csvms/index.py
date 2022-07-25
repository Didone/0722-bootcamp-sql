""" Index Module """
from csvms import logger

log = logger()

class Node():
    """Represents a table index"""

    def __init__(self, key, data) -> None:
        self.left = None
        self.right = None
        self.key = key
        self.data = data

    def insert(self, key, data):
        """Insert nodes"""
        if self.key:
            if key < self.key:
                if self.left is None:
                    self.left = Node(key, data)
                else:
                    self.left.insert(key, data)
            elif key > self.key:
                if self.right is None:
                    self.right = Node(key, data)
                else:
                    self.right.insert(key, data)
        else:
            self.data = data

    def show(self):
        """Print tree"""
        if self.left:
            self.left.show()
        print(f"{self.key}:{self.data}")
        if self.right:
            self.right.show()

    def search(self, key):
        """Search on tree"""
        if key < self.key:
            log.info("search:left:%s",self.key)
            if self.left is None:
                raise ValueError(f"Key {key} not found")
            return self.left.search(key)
        elif key > self.key:
            log.info("search:right:%s",self.key)
            if self.right is None:
                raise ValueError(f"Key {key} not found")
            return self.right.search(key)
        else:
            log.info("search:found:%s",self.key)
            return self
