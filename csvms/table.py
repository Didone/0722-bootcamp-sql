""" Table Module """
import json
import pickle
import re
from csv import reader, writer
from datetime import datetime
from os import remove, makedirs
from os.path import exists
from pathlib import Path
from typing import List, Dict, Any
# Local module
from csvms import logger
from csvms.schema import Database
from csvms.exceptions import ColumnException
from csvms.exceptions import DataException
from csvms.exceptions import TableException
from csvms.index import Node

# Init log
log = logger()

def rnm(table:str, column:str) -> str:
    """Rename table column
    :param table: Table name
    :param column: Column name
    :return: Column renamed
    """
    if str(column).find('.')!=-1:
        return column
    return f"{table}.{column}"

def _nan_(value:Any) -> bool:
    """Check for None values
    :param value: The value to check
    :return: False if value is None
    """
    if value is None:
        return False
    return value

class Table():
    """
    Represents a collection of tuples as table

    Class variables
    ----------
    dtypes: Supported columns data types
    operations: Supported operator on selection operations
    functions: Supported functions on extended operations

    Attributes
    ----------
    name: Table name identifier
    database: Database where the table are located
    columns: Table attributes
    temporary: True if table is temporary.
    """
    _FORMAT_="csv" # Data file format
    _CSVSEP_=";"   # Separator
    # Supported data types
    dtypes = {
        "string":str,
        "text":str,
        "int":int,
        "integer":int,
        "float":float,
        "boolean":bool}
    # Supported operations
    operations = {
        'lt'     :lambda x,y:_nan_(x) < _nan_(y),
        'gt'     :lambda x,y:_nan_(x) > _nan_(y),
        'eq'     :lambda x,y:_nan_(x) == _nan_(y),
        'lte'    :lambda x,y:_nan_(x) <= _nan_(y),
        'gte'    :lambda x,y:_nan_(x) >= _nan_(y),
        'neq'    :lambda x,y:_nan_(x) != _nan_(y),
        'is'     :lambda x,y:_nan_(x) is _nan_(y),
        'in'     :lambda x,y:_nan_(x) in _nan_(y),
        'nin'    :lambda x,y:_nan_(x) not in _nan_(y),
        'or'     :lambda x,y:_nan_(x) or _nan_(y),
        'and'    :lambda x,y:_nan_(x) and _nan_(y),
        'missing':lambda   x:x is None,
        'exists' :lambda   x:x is not None}
    # Supported functions
    functions = {
        'add': lambda x,y: None if x is None or y is None else x+y,
        'sub': lambda x,y: None if x is None or y is None else x-y,
        'div': lambda x,y: None if x is None or y is None else x/y,
        'mul': lambda x,y: None if x is None or y is None else x*y,
        'ifnull': lambda x,y: y if x is None else x,
        'coalesce': lambda x,y: y if x is None else x,
        'concat': lambda x,y: None if x is None or y is None else str(x)+str(y),
        'pow': lambda x,y: None if x is None or y is None else pow(x,y),
    }
    # Supported operations in reverse
    _strtypes_ = {value:key for key, value in dtypes.items()}

    def __init__(self, name:str, columns:Dict[str,type]=None,
        data:List[tuple]=None, temp:bool=False):
        """
        Table representation and the data file using database location path to store all rows
        :param name:
            Table identifier composed by database name and the table name separated by '.'
            If the database name was omitted, uses the default database instead
        :pram columns:
            Dictionary with columns names and data types. Only python primitive type are alowed.
            If None, load from catalog definition. *Default is None*

            # Example
            ```
            Table(
                name='sample',
                columns={
                    'att1':str,
                    'att2':int})
            ```
        :param data:
            Load table tuples into table rows. If None load from data file. *Default is None*

            #### Example
            ```
            Table(
                name='sample',
                columns={
                    'att1':str,
                    'att2':int},
                data=[
                    ('a',1),
                    ('b',2)])
            ```
        :param temp :
            If 'False' create datafile, other else the rows will be available only on python memory.
            *Default False*
        """
        self.index = dict()
        self.journal = list()
        self.temporary = temp
        _db = None
        self.name = name
        if name.count('.') == 1:
            _db, self.name = name.split('.')
        self.database = Database(_db, temp)
        self.columns = columns
        if data is not None:
            self._rows = data
            for row in data:
                self._redo_(('I',(Table._op_ts_()))+row)
        else:
            self._rows = list()
            if exists(self.location):
                self._rows = list(self.load())
        if self.columns is None:
            raise TableException(f"Table {name} not found")

    @classmethod
    def _op_ts_(cls) -> str:
        """Get the system date time and format
        :return: Return a formatted timestamp
        """
        return datetime.today().isoformat().replace('T',' ')

    @classmethod
    def _condition_parser_(cls, exp:str) -> List[str]:
        """Condition parser
        :param exp: String with operation
        :return: List with operation name and value
        """
        ops = '|'.join(Table.operations.keys())
        match = next(re.finditer(rf"({ops})\s+(.+)", exp, re.IGNORECASE))
        return [match.group(1), match.group(2)]

    @property
    def full_name(self):
        """Returns table full name identifier"""
        return f"{self.database.name}.{self.name}"

    @property
    def definition(self) -> dict:
        """Returns table definition as dictionary"""
        return dict(
            name=self.full_name,
            columns = {k: Table._strtypes_[v] for k, v in self.columns.items()},
            indexes= {k:v.location for k,v in self.index.items()}
        )

    @property
    def location(self) -> str:
        """Returns table location on file system as string"""
        return f"{self.database.location}/{self.name}.{Table._FORMAT_}"

    @property
    def empty_row(self) -> tuple:
        """Returns an tuple with 'None' values for each column"""
        return tuple([None for _ in self.columns])

    @property
    def journal_path(self) -> Path:
        """Returns Path to transaction log"""
        return Path(f"{Database.FILE_DIR}/log/{self.full_name}")

    def _redo_(self, values:tuple) -> None:
        """Write transaction redo log file
        :param values: Tuple of values"""
        self.journal.append(values)

    def _value_(self, row:tuple, key:str):
        """Get value from row by column name if it's a columnn identifier
        :param row: Row tuple
        :param key: Column identifier
        :return: attribute value"""
        if key in self.columns.keys():
            return row[key]
        return key

    def add_index(self, index:"Index"):
        """Update table indexes
        :param index: New table index"""
        self.index.update({index.name:index})

    def load(self) -> List[tuple]:
        """Load csv file from path with column formats
        :param table_id: Table full name
        :return: Tuple iterator"""
        definition = self.database.catalog[self.full_name]
        # Load column definitions
        self.columns = {key:Table.dtypes[value] for key, value in definition["columns"].items()}
        # Load indexes, if exists
        for key, value in definition["indexes"].items():
            with open(f"{value}/{key}", 'rb') as index:
                self.index.update({key:pickle.load(index)})
        # Load data, if exists
        with open(self.location, mode='r', encoding="utf-8") as csv_file:
            for raw in reader(csv_file, delimiter=Table._CSVSEP_):
                row = list()
                for idx, col in enumerate(self.columns.values()):
                    row.append(col(raw[idx]))
                yield tuple(row)

    def save(self) -> bool:
        """Write data to file system
        :return: True if was succeeded"""
        if self.temporary:
            raise TableException("Can't save temporary tables")
        # Transaction log
        makedirs(self.journal_path, exist_ok=True)
        log_file = self.journal_path.joinpath("redo")
        with open(log_file, mode='a', encoding="utf-8") as redolog:
            for values in self.journal:
                writer(redolog).writerow(values)
            self.journal = list()
        # Save table index
        for name, idx in self.index.items():
            idx_location = Path(idx.location)
            makedirs(idx_location, exist_ok=True)
            idx_file = idx_location.joinpath(name)
            with open(idx_file, mode='wb') as index:
                pickle.dump(idx.update(self), index)
        # Table data
        with open(self.location, mode='w', encoding="utf-8") as csv_file:
            csv_writer = writer(csv_file, delimiter=Table._CSVSEP_, quotechar='"')
            for row in self._rows:
                csv_writer.writerow(row)
        self.database.catalog[self.full_name] = self.definition
        return True

    def alter(self, option:str, column:Dict[str,type], new:Dict[str,type]=None) -> "Table":
        """Alter table definitions
        :param option: Accepts ADD, DROP and MODIFY
        :param column: Where to apply alteration
        :param new: New column definition. Only used on MODIFY operations. Default is None
        :return: The new modified table"""
        for key, val in column.items():
            if option.upper() == "ADD":
                return self._add_column_(key, val)
            if option.upper() == "DROP":
                return self._drop_column_(key)
            if option.upper() == "MODIFY":
                if new is None:
                    raise ColumnException("Need to inform new column definition")
                return self._modify_column_(key, new)
        raise ColumnException(f"Column {column} not found")

    def _add_column_(self, name:str, dtype:type) -> "Table":
        """Add new column to table
        :param name: Column name
        :param dtype: Column data type
        :return: The new modified table"""
        self.columns.update({name:dtype}) # Add column definition
        for idx, row in enumerate(self._rows):
            self._rows[idx] = row + (dtype(),) # Add default values
        return self.save()

    def _drop_column_(self, column:str) -> "Table":
        """Drop column from table
        :param key: Column name
        :return: The new modified table"""
        idx = None
        for pos, col in enumerate(self.columns.keys()):
            if col == column:
                idx = pos # Save colum index
                del self.columns[column] # Remove from columns
                break # exit from loop
        if idx is None:
            raise ColumnException(f"Column {column} not found")
        for pos, row in enumerate(self._rows):
            row = list(row) # Convert to list
            del row[idx] # remove value for column index
            self._rows[pos] = tuple(row) # Update row
        return self.save()

    def _modify_column_(self, name:str, column:Dict[str,type]) -> "Table":
        """Drop column from table
        :param name: Column name
        :param column: New column
        :return: The new modified table"""
        idx = None
        val = next(iter(column.values()))
        for pos, col in enumerate(self.columns.keys()):
            if col == name:
                idx = pos # Save colum index
                self.columns.update(column) # Update definition
                del self.columns[col] # Remove old column
                break # exit from loop
        tmp_rows = self._rows
        try:
            for pos, row in enumerate(self._rows):
                row = list(row)
                row[idx] = val(row[idx]) # Update column value
                self._rows[pos] = tuple(row) # Update row list
        except Exception as err:
            log.debug(err)
            self._rows = tmp_rows
            raise ColumnException(f"Cant change data type for column {name}")
        return self.save()

    def clean(self) -> bool:
        """Remove all table data
        :return: True if was succeeded"""
        self._rows = list()
        if exists(self.location):
            remove(self.location)
        if not self.temporary:
            Path(self.location).touch()
        return True

    def drop(self) -> bool:
        """Remove physical file
        :return: True if was succeeded"""
        remove(self.location)
        del self.database.catalog[self.full_name]
        return True

    def show(self, size:int=20, trunc:bool=True) -> str:
        """Print as pretty table data
        :param size: Maximum number of lines to print
        :param trunc: If true remove de full column name.
                      For example 'table.column1' will be printed like 'column1'
        :return: String table representaion"""
        # Ugly code for a pretty table...
        idx_pad = 3
        # Max size of each column
        col_size = dict()
        for _c_ in self.columns:
            col_size[_c_]=len(_c_)+1
            for idx, _ in enumerate(self):
                cols = len(str(self[idx][_c_]))
                if col_size[_c_] < cols:
                    col_size[_c_]= cols
        # Table line separator
        sep = f"{' ':{'>'}{idx_pad}}+"
        for key in self.columns.keys():
            sep += f"{'-':{'-'}{'<'}{col_size[key]}}+"
        # Table header
        col = f"{' ':{'>'}{idx_pad}}|"
        for key in self.columns.keys():
            if trunc:
                col += f"{key.split('.')[-1]:{'<'}{col_size[key]}}|"
            else:
                col += f"{key:{'<'}{col_size[key]}}|"
        # Table rows
        rows = str()
        if len(self) <= size:
            size = len(self)
            for idx in range(size):
                rows += f"{idx:{''}{'>'}{idx_pad}}|"
                for key, val in self[idx].items():
                    rows += f"{str(val):{'>'}{col_size[key]}}|"
                rows+='\n'
        else:
            for idx in range(int(size/2)):
                rows += f"{idx:{''}{'>'}{idx_pad}}|"
                for key, val in self[idx].items():
                    rows += f"{str(val):{'>'}{col_size[key]}}|"
                rows+='\n'
            # Separator row,
            rows += f"{' ':{'>'}{idx_pad}}|"
            for key in self.columns.keys():
                rows += f"{'...':{''}{'>'}{col_size[key]}}|"
            rows+='\n'
            #reversed rows
            for idx in reversed(range(int(size/2))):
                _idx = len(self)-idx-1
                rows += f"{_idx:{''}{'>'}{idx_pad}}|"
                for key, val in self[_idx].items():
                    rows += f"{str(val):{'>'}{col_size[key]}}|"
                rows+='\n'
        tbl = f"TABLE: {self.full_name}\n"
        if len(rows)>0:
            return f"""{tbl}{sep}\n{col}\n{sep}\n{rows[:-1]}\n{sep}\n"""
        return f"""{tbl}{sep}\n{col}\n{sep}\n{sep}\n"""

    def _validade_(self, value) -> tuple:
        """Check if the values are compatible with column data type
        :param value: Tuple of values to check
        :return: Tuple with cast in all values according to the data type"""
        row = tuple()
        try:
            for idx, val in enumerate(self.columns.values()):
                if value[idx] is None:
                    row += (None,)
                else:
                    row += (val(value[idx]),)
            return row
        except IndexError as err:
            log.debug(err)
        except ValueError as err:
            log.debug(err)
        raise DataException(f"Invalid data {value} to row {tuple(self.columns.values())}")

    def append(self, *values) -> bool:
        """Add new row
        :param values: list of values, separated by comma, to insert into
        :return: True if table insertion was succeeded"""
        self._rows.append(self._validade_(values))
        self._redo_(('I',(Table._op_ts_()))+tuple(values))
        if not self.temporary:
            log.info("Row %s inserted", values)
        return True

    def __setitem__(self, idx:int, value:tuple) -> bool:
        """Update row
        :param idx: Index row to update
        :param value: New values to the row
        :return: True if was succeeded"""
        self._rows[idx] = self._validade_(value)
        self._redo_(('U',(Table._op_ts_()))+tuple(value))
        if not self.temporary:
            log.info("Row %s updated with values %s", idx, value)
        return True

    def __delitem__(self, idx) -> None:
        """Remove line from table
        :param idx: Row table index to delete"""
        data = self._rows[idx]
        self._redo_(('D',(Table._op_ts_()))+data)
        del self._rows[idx]
        if not self.temporary:
            log.info("Row %s deleted", data)

    def __getitem__(self, key):
        """Get table row
        :param key: Row index
        :return: Row as dict"""
        try:
            return {n:self._rows[key][i] for i,n in enumerate(self.columns)}
        except IndexError:
            log.debug("Row %s not found", key)
            return {col:None for col in self.columns.keys()}

    def __iter__(self) -> iter:
        """Get all table rows
        :return: Row iterator"""
        for row in self._rows:
            yield row

    def __len__(self):
        """Returns number of table rows"""
        return len(self._rows)

    def __repr__(self):
        """Returns table definition in JSON format"""
        return json.dumps(self.definition)

    def __str__(self):
        """Pretty table format"""
        return self.show()

    def extend(self, row:dict, ast:dict):
        """ Resolve functions recursively
        :param ast: parsed expression
        :return: Calculated value"""
        if isinstance(ast, dict):
            for key, val in ast.items():
                _x_, _y_ = val
                if isinstance(_x_, dict):
                    if _x_.get('literal') is None:
                        _x_ = self.extend(row, _x_)
                    else:
                        _x_ = _x_['literal']
                if isinstance(_y_, dict):
                    if _y_.get('literal') is None:
                        _y_ = self.extend(row, _y_)
                    else:
                        _y_ = _y_['literal']
                return Table.functions[key](self._value_(row,_x_),self._value_(row,_y_))
        return ast

    def logical_evaluation(self, row:dict, ast:dict) -> bool:
        """Recursively evaluate conditions
        :param ast: Abstract Syntax Tree
        :return: Boolean result"""
        if isinstance(ast, dict):
            for key, val in ast.items():
                if key in ['missing','exists']:
                    return Table.operations[key](self._value_(row,val))
                if len(val)>2: # Multiple conditions with and/or
                    return self.logical_evaluation(row, {key:[val[-2],val[-1]]})
                _x_, _y_ = val
                if isinstance(_x_, dict):
                    if _x_.get('literal') is None:
                        _x_ = self.logical_evaluation(row, _x_)
                    else:
                        _x_ = _x_['literal']
                if isinstance(_y_, dict):
                    if _y_.get('literal') is None:
                        _y_ = self.logical_evaluation(row, _y_)
                    else:
                        _y_ = _y_['literal']
                return Table.operations[key](self._value_(row,_x_),self._value_(row,_y_))
        raise DataException(f"Can't evaluate expression: {ast}")

    ### Relational Algebra operators ###

    def __add__(self, other:"Table") -> "Table":
        """Union Operator (∪)"""
        return Table(
            name = f"({self.name}∪{other.name})",
            # Copy all columns from self
            columns={k:v for k,v in self.columns.items()},
            # Sum all distinct rows from self and other table
            data= list(dict.fromkeys(self._rows + other._rows)))

    def __mod__(self, other:"Table") -> "Table":
        """Inserct Operator (∩)"""
        return Table(
            name = f"({self.name}∩{other.name})",
            # Copy all columns from self
            columns={k:v for k,v in self.columns.items()},
            # Filter rows of self equal to rows of other
            data=[r for r in self for o in other if r == o])

    def __sub__(self, other:"Table") -> "Table":
        """Difference Operator (−)"""
        rows = list() # Create a new list of rows
        for _r_ in self: # For each row in self
            rows.append(_r_) # Add the self rows to the new list
            for _o_ in other: # Check if are any tuple in other table that match
                if _r_ == _o_: # If finds a row in other that are equal to self
                    try:
                        rows.pop() # Remove self rows
                    except IndexError:
                        continue
        return Table(
            name = f"({self.name}−{other.name})",
            columns={k:v for k,v in self.columns.items()},
            data=rows)

    def __mul__(self, other:"Table") -> "Table":
        """Times Operator (×)"""
        # Join the tow sets of columns, and concatenate the table name if needed
        columns = {rnm(self.name,k):v for k, v in self.columns.items()}
        columns.update({rnm(other.name,k):v for k, v in other.columns.items()})
        return Table(
            name = f"({self.name}×{other.name})",
            # Concatenated columns
            columns=columns,
            # Cartesian product of a set of self rows with a set of other rows
            data=[r+o for r in self for o in other])

    #TODO: Implement DivideBy operator
    # More info: https://en.wikipedia.org/wiki/Relational_algebra#Division_(%C3%B7)

    def π(self, select:list) -> "Table":
        """Projection Operator (π)"""
        # Create a list of projected columns and your index
        cols = {k:v for k,v in self.columns.items()}
        rows = [v for v in self]
        if select != '*': #In case of '*' return all columns
            if not isinstance(select, list):
                raise NotImplementedError
            _tc = list() # List of orderd columns and indexes
            for col in select:
                # Get column index
                for _i_,_c_ in enumerate(self.columns.keys()):
                    if col['value']==_c_: #When find the column
                        _a_ = _c_
                        if col.get('name') is not None:
                            _a_ = col['name']
                        _tc.append((_i_,_c_,_a_)) #Add to the list with the index
                        break #Exit from loop when find
                    elif col.get('name')==_c_:
                        _a_ = _c_ = col['name']
                        _tc.append((_i_,_c_,_a_)) #Add to the list with the index
            if len(_tc)!=len(select):
                raise ColumnException("Cant find all columns")
            rows = list()
            for row in self: # For each row
                _r_ = tuple() # Create a new tuple
                for idx,_,_ in _tc: # For each projected column
                    _r_ += (row[idx],) # Add values for projected column index
                rows.append(_r_) # Append the new sub tuple to the new list of rows
            cols = {a:self.columns[k] for _,k,a in _tc}
        return Table(name=f"({self.name}π)",columns=cols,data=rows)

    def σ(self, condition:Dict[str,list], null=False) -> "Table":
        """Selection Operator (σ)
        :param condition: A expression composed by the logic operation and list of values.
                          See 'operations' dictionary to get the list of valid options
        # Exemples

        where id < 2
        `where({'lt':['id',2]})`

        where val = 'George' and id > 1
        `where({'and':[{"eq":['val','George']},{"gt":['id',1]}]})`

        # Operations
        List of supported operations and the logical equivalent python evaluation

        | Name    | Python eval |
        |---------|-------------|
        | lt      | <           |
        | gt      | >           |
        | eq      | ==          |
        | lte     | <=          |
        | gte     | >=          |
        | neq     | !=          |
        | is      | is          |
        | in      | in          |
        | nin     | not in      |
        | or      | or          |
        | and     | and         |
        | missing | is None     |
        | exists  | is not None |

        """
        rows = list()
        for idx, row in enumerate(self):
            if self.logical_evaluation(self[idx], condition):
                rows.append(row)
        if null and len(rows)==0:
            rows.append(self.empty_row)
        return Table(
            name = f"({self.name}σ)",
            columns={k:v for k,v in self.columns.items()},
            data=rows)

    def ᐅᐊ(self, other:"Table", where:Dict[str,list]) -> "Table":
        """Join Operator (⋈)"""
        # Create a new table with the Cartesian product of self and otther
        tbl = (self * other)\
            .σ(where) # And select rows where the join condition is true
        tbl.name = f"({self.name}⋈{other.name})"
        return tbl

    def ρ(self, alias:str) -> "Table":
        """Rename Operator (ρ)"""
        # Function to rename column names for the new table name
        rename = lambda x: x if x.count('.')==0 else x.split('.')[-1]
        return Table(
            # Set new table name
            name = f"{alias}",
            # Copy all columns from source table
            columns={rename(k):v for k,v in self.columns.items()},
            # Copy all rows from source table
            data=[r for r in self])

    def Π(self, extend:dict, alias:str=None) -> "Table":
        """Extended projection Operator (Π)"""
        rows = list() # New list of rows
        dtype = None # Use to store the data type of the new extended column
        for idx, row in enumerate(self): # For each row
            val = self.extend(self[idx],extend) # Evaluated expression
            if dtype is None: # If is the first evaluation
                dtype = type(val) # Use the result data type
            # if you find any different type in the next rows
            elif dtype != type(val) and val is not None:
                # Raise an Data exeption to abort the operation
                raise DataException(f"{type(val)} error")
            # If Successful add new value to the row tuple
            rows.append(row + (val,))
        # Copy the columns from source table
        cols = {k:v for k,v in self.columns.items()}
        # Add new extended column
        if alias is None: # Remova some characters and use the expression as column name
            cols[f"{str(extend).replace(' ','').replace('.',',')}"]=dtype
        else: # Use alias for the new extended column
            cols[alias]=dtype
        return Table(
            name = f"({self.name}Π)",
            columns=cols,
            data=rows)

    #TODO: Implement FULL join operator `ᗌᗏ`
    #TODO: Implement LEFT SEMI join operator `ᐅᐸ`
    #TODO: Implement RIGHT SEMI join operator `ᐳᐊ`
    #TODO: Implement LEFT ANTI join operator `ᐅ`
    #TODO: Implement RIGHT ANTI join operator `◁`

class Index():
    """Represents a table index"""
    def __init__(self, name:str, table:"Table", attribute:str) -> None:
        """Create index from table
        :param name: Index name
        :param table: Table object where the index will be created
        :param attribute: Column name to index"""
        self.name = name
        self.attribute = attribute
        self.location = f"{table.database.location}/.index/{table.name}"
        self.tree = self._tree_(table)
        #Update table definition with index
        table.add_index(self)

    def _tree_(self, table:"Table") -> Node:
        """Create index tree
        :param table: Table object to create the index tree
        :return: Tree nodes"""
        _root:Node = None
        for idx, _ in enumerate(table):
            key = table[idx][self.attribute]
            if _root is None:
                _root = Node(key, idx)
            else:
                _root.insert(key, idx)
        return _root

    def update(self, table:"Table") -> "Index":
        """Recreate index based on table data
        :param table: Table object to update
        :return: Updated index"""
        self.tree = self._tree_(table)
        return self

    def search(self, key) -> Node:
        """Search value in table index
        :param key: Value to find
        :return: Row index where the key is located"""
        try:
            return self.tree.search(key).data
        except ValueError as err:
            log.error(err)
