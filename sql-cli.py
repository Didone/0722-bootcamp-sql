from csvms.engine import Engine

eng = Engine()
sql = """"""
print('\n-- Started SQL CLI --')
print('Type "exit" to exit')
print('Type "help" to get help\n')

while(True):
    sql = input("> ")
    if sql.strip() == 'help':
        print(f"\nCLI Commands:\n\t{'help':<20} - show this help\n\t{'exit':<20} - exit the program\n\nSQL Commands:\n\t{'create table':<20} - create a table\n\t{'insert':<20} - insert into a table\n\t{'update':<20} - update a table\n\t{'select':<20} - select from a table\n\t{'select distinct':<20} - select distinct rows from a table\n\t{'delete':<20} - delete from a table\n\t{'commit':<20} - commit changes to a table\n\t{'rollback':<20} - rollback all changes\n\n\tJoins:\n\t\t{'inner join':<20} - inner join two tables\n\t\t{'left join':<20} - left join two tables\n\t\t{'right join':<20} - right join two tables\n\t\t{'full join':<20} - full join two tables\n\n REMINDER: PUT SEMICOLONS (;) BETWEEN COMMANDS\n\n")
    if sql.strip() == 'exit':
        break
    try:
        eng.execute(sql)
    except Exception as e:
        print(f"\n{e}\n")
        continue
