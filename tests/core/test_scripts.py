"""Test for bootcamp examples"""
from os.path import exists
from csv import reader
from csvms.table import Table
from csvms.table import Database

def load(table) -> list:
    """Load csv file from path with column formats
    :param table_id: Table full name
    :return: Tuple iterator
    """
    definition = table.database.catalog[table.full_name]
    table.columns = {key:Table.dtypes[value] for key, value in definition["columns"].items()}
    with open(table.location, mode='r', encoding="utf-8") as csv_file:
        for raw in reader(csv_file, delimiter=Table._CSVSEP_):
            row = list()
            for idx, col in enumerate(table.columns.values()):
                row.append(col(raw[idx]))
            yield tuple(row)


def test_dml_ddl():
    """Test dml-ddl scripts"""
    Table("mock_frutas",{'nm_fruta':str,'tp_fruta':str}).save()
    assert not exists(f"{Database.FILE_DIR}/{Database.DEFAULT_DB}/mock_frutas")
    # Load
    tbl=Table("mock_frutas")
    assert tbl.definition == {
        'name': 'mock.mock_frutas',
        'columns': {'nm_fruta': 'text', 'tp_fruta': 'text'},
        'indexes': {}}
    # Insert
    tbl.append('banana','doce')
    tbl.append('limão','azedo')
    tbl.append('maçã','doce')
    assert [r for r in tbl] == [('banana','doce'),('limão','azedo'),('maçã','doce')]
    tbl.save() # Commit
    assert list(load(tbl)) == [('banana','doce'),('limão','azedo'),('maçã','doce')]
    # Delete
    for idx in range(len(tbl)):
        if tbl[idx]["nm_fruta"]=="limão":
            del tbl[idx]
    assert [r for r in tbl] == [('banana','doce'),('maçã','doce')]
    # Update
    for idx in range(len(tbl)):
        if tbl[idx]["tp_fruta"]=='doce':
            row = tbl[idx]
            row["tp_fruta"]='amargo'
            tbl[idx] = tuple(row.values())
    assert [r for r in tbl] == [('banana','amargo'),('maçã','amargo')]
    # Rollback
    tbl = Table('mock_frutas')
    assert [r for r in tbl] == [('banana','doce'),('limão','azedo'),('maçã','doce')]

    tbl.alter('ADD',{'valor':int})
    assert [r for r in tbl] == [('banana','doce',0),('limão','azedo',0),('maçã','doce',0)]
    # Alter table MODIFY
    tbl.alter('MODIFY',{'valor':int},{'vl_fruta':float})
    assert [r for r in tbl] == [('banana','doce',0.0),('limão','azedo',0.0),('maçã','doce',0.0)]
    # Alter table DROP
    tbl.alter('DROP',{'vl_fruta':float})
    assert [r for r in tbl] == [('banana','doce'),('limão','azedo'),('maçã','doce')]
