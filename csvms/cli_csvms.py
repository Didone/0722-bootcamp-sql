from csvms.engine import Engine
from csvms.table import Table
import cmd
from docopt import docopt

#!/usr/bin/env python
__doc__ = """
This example uses docopt with the built in cmd module to demonstrate an
interactive command application.
Usage:
    cli_csvms.py execute <SQL>
    cli_csvms.py (-i | --interactive)
    cli_csvms.py (-h | --help | --version)
Options:
    -i, --interactive  Interactive Mode
    -h, --help  Show this screen and exit.
"""


class MyInteractive (cmd.Cmd):
    intro = 'Welcome to my interactive program!' \
        + ' (type help for a list of commands.)'
    prompt = '> '
    file = None

    def do_execute(self, arg):
        """Usage: execute <SQL>"""
        table = Engine().execute(arg)
        print(table)

    def do_quit(self, arg):
        """Quits out of Interactive Mode."""

        print('Good Bye!')
        exit()


opt = docopt(__doc__) 

if opt['--interactive']:
    MyInteractive().cmdloop()

print(opt)