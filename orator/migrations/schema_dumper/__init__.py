import autopep8
from importlib import import_module


def dump(conn):
    Dumper = import_module(
        '{}.{}_dumper'.format(__package__, conn.name)).Dumper
    output = Dumper(conn, conn.get_default_schema_grammar(),
                    conn.get_database_name()).dump()
    valid_output = autopep8.fix_code(output)
    print(valid_output)
    return valid_output
