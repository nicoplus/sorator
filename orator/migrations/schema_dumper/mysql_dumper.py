import re
from operator import itemgetter
from collections import namedtuple, defaultdict
from .dumper_interface import Dumper as BaseDumper


class Dumper(BaseDumper):

    __ignore_list__ = 'migrations'

    column_record = namedtuple('Record', ['name', 'ttype', 'precision',
                                          'unsigned', 'nullable', 'default',
                                          'extra'])

    mapping = {
        'BIGINT': 'big_integer',
        'BLOB': 'binary',
        'BOOLEAN': 'boolean',
        'CHAR': 'char',
        'DATE': 'date',
        'DATETIME': 'datetime',
        'DECIMAL': 'decimal',
        'DOUBLE': 'double',
        'ENUM': 'enum',
        'FLOAT': 'float',
        'INT': 'integer',
        'JSON': 'json',
        'LONGTEXT': 'long_text',
        'MEDIUMINT': 'medium_int',
        'MEDIUMTEXT': 'medium_text',
        'SMALLINT': 'small_int',
        'TEXT': 'text',
        'TIME': 'time',
        'TINYINT': 'tiny_int',
        'TIMESTAMP': 'timestamp',
        'VARCHAR': 'string'
    }

    def handle_column(self, columns):
        statements = []
        for column in columns:
            column_buffer = []
            name = column.name
            ttype = self.mapping[column.ttype.upper()]
            unsigned = column.unsigned
            precision = column.precision

            # bigint auto_increment -> big_increments
            # int auto_increment -> increments
            pk = False
            if column.extra == 'auto_increment':
                if ttype == 'big_integer':
                    ttype = 'big_increments'
                    pk = True
                if ttype == 'integer':
                    ttype = 'increments'
                    pk = True

            if ttype == 'tiny_int' and precision == 1:
                ttype = 'boolean'
                precision = None

            # tiny_int when length is 1 -> boolean
            if ttype == 'tiny_int' and column.precision == 1:
                ttype = 'boolean'

            nullable = column.nullable
            default = column.default

            # dump to orator schema syntax
            if not pk and precision:
                column_buffer.append(
                    'table.{ttype}({name}, {precision})'.format(
                        ttype=ttype, name=repr(name),
                        precision=repr(precision)))
            else:
                column_buffer.append('table.{ttype}({name})'.format(
                    ttype=ttype, name=repr(name)))
            if not pk and unsigned == 'unsigned':
                column_buffer.append('.unsigned()')
            if nullable != 'NO':
                column_buffer.append('.nullable()')
            if default is not None:
                flag = True
                # ignore timestamp type default value CURRENT_TIMESTAMP(6)
                if ttype == 'timestamp' and \
                        default.startswith('CURRENT_TIMESTAMP'):
                    flag = False

                if flag:
                    default = default.strip("'")
                    if default.isdigit():
                        default = int(default)
                    elif re.match("^\d+?\.\d+?$", default) is not None:
                        default = float(default)
                    else:
                        default = "'{}'".format(default)
                    column_buffer.append('.default({})'.format(default))
            statements.append(''.join(column_buffer))
        return statements

    def handle_index(self, indexes):
        statements = []
        for name, index in sorted(indexes.items(), key=itemgetter(0)):
            ttype = 'index'
            if index['is_unique']:
                ttype = 'unique'
            if name == 'PRIMARY':
                ttype = 'primary'
                name = None

            statements.append(
                'table.{}({}, name={})'.format(ttype,
                                              repr(index['columns']),
                                              repr(name)))
        return statements

    def handle_foreign_key(self, foreign_keys):
        statements = []
        for foreign_key in foreign_keys:
            # name = foreign_key['name']
            local_key = foreign_key['column']
            ref_key = foreign_key['ref_key']
            to_table = foreign_key['to_table']
            on_update = foreign_key['on_update']
            on_delete = foreign_key['on_delete']

            statement = 'table.foreign({}).references({}).on({})'.format(
                repr(local_key), repr(ref_key), repr(to_table))

            if on_update.upper() == 'CASCADEA':
                statement += ".on_update('cascadea')"

            if on_delete.upper() == 'CASCADEA':
                statement += ".on_delete('cascadea')"

            statements.append(statement)
        return statements

    def list_tables(self):
        """list all table_names from specified database
        rtype [str]
        """
        sql = self._grammar._list_tables()
        result = self._conn.select(sql)
        return filter(
            lambda table_name: table_name not in self.__ignore_list__,
            map(itemgetter('table_name'), result))

    def list_columns(self, table_name):
        """list column in table
        rtype [namedtuple]"""
        sql = self._grammar._list_columns(table_name)
        result = self._conn.select(sql)
        return [self.column_record(**r) for r in result]

    def list_indexes(self, table_name):
        """list index in table"""
        sql = self._grammar._list_indexes(table_name)
        result = self._conn.select(sql)
        indexes = defaultdict(lambda: {'columns': [], 'is_unique': False})
        for r in result:
            index = indexes[r['Key_name']]
            index['columns'].append(r['Column_name'])
            if not r['Non_unique']:
                index['is_unique'] = True
        return indexes

    def list_foreign_keys(self, table_name):
        """list foreign key from specified table"""
        sql = self._grammar._list_foreign_keys(table_name)
        result = self._conn.select(sql)
        return result
