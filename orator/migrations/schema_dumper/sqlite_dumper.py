import re
from operator import itemgetter
from collections import namedtuple, defaultdict
from .dumper_interface import Dumper as BaseDumper


class Dumper(BaseDumper):

    __ignore_list__ = ['migrations', 'sqlite_sequence']

    column_record = namedtuple('Record', ['name', 'ttype', 'nullable', 'pk',
                                          'precision', 'default', 'autoincr'])

    mapping = {
        'INT': 'integer',
        'SMALLINT': 'small_int',
        'TINYINT': 'tiny_int',
        'MEDIUMINT': 'medium_int',
        'BIGINT': 'big_integer',
        'UNSIGNED BIGINT': 'big_integer',
        'FLOAT': 'float',
        'DECIMAL': 'decimal',
        'DOUBLE': 'double',
        'REAL': 'double',
        'BLOB': 'binary',
        'CHAR': 'char',
        'VARCHAR': 'string',
        'MEDIUMTEXT': 'medium_text',
        'TEXT': 'text',
        'LONGTEXT': 'long_text',
        'DATE': 'date',
        'DATETIME': 'timestamp',  # SQLite doesn't have TIMESTAMP type
        'INTEGER': 'integer',
        'JSON': 'json',
    }

    def list_columns(self, table_name):
        """list column in table
        rtype [namedtuple]"""
        sql = self._grammar._list_columns(table_name)
        result = self._conn.select(sql)
        autoincrement_columns = self.list_autoincrement_columns(table_name)
        for r in result:
            # add autoincrement info
            if r['name'] in autoincrement_columns:
                r['autoincr'] = True
            else:
                r['autoincr'] = False

            precision = None
            # handle type precision
            match_obj = re.match(r'\w+\((\S+)\)', r['type'])
            if match_obj is not None:
                # VARCHAR(20)
                precision, *_ = match_obj.groups()
                precision = int(precision)

        return [self.column_record(
            name=r['name'],
            ttype=r['type'],
            precision=precision,
            pk=r['pk'],
            nullable=not r['notnull'],
            default=r['dflt_value'],
            autoincr=r['autoincr']
        ) for r in result]

    def handle_column(self, columns):
        statements = []
        for column in columns:
            column_buffer = []
            name = column.name
            ttype = self.mapping[column.ttype.upper()]

            if column.autoincr:
                if ttype == 'big_integer':
                    ttype = 'big_increments'
                if ttype == 'integer':
                    ttype = 'increments'

            nullable = column.nullable
            default = column.default

            # dump to orator schema syntax
            column_buffer.append('self.{ttype}({name})'.format(
                ttype=ttype, name=repr(name)))
            if nullable:
                column_buffer.append('.nullable()')
            if default is not None:
                flag = True
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

    def list_indexes(self, table_name):
        """list index in table"""
        sql = self._grammar._list_indexes(table_name)
        indexes = defaultdict(lambda: {'columns': [], 'is_unique': False})
        result = self._conn.select(sql)
        for r in result:
            index_name = r['name']
            indexes[index_name]['is_unique'] = bool(r['unique'])
            columns = map(itemgetter('name'),
                          self._conn.select(
                              self._grammar._show_index(index_name)))
            indexes[r['name']]['columns'].extend(columns)
        return indexes

    def handle_index(self, indexes):
        statements = []
        for name, index in indexes.items():
            ttype = 'index'
            if index['is_unique']:
                ttype = 'unique'
            if name == 'PRIMARY':
                ttype = 'primary'
                name = None

            statements.append(
                'self.{}({}, name={})'.format(ttype,
                                              repr(index['columns']),
                                              repr(name)))
        return statements

    def list_autoincrement_columns(self, table_name):
        # SQLite autoincrement is not recommanded to use
        # see https://sqlite.org/autoinc.html
        # Besides it have none default value
        result = []
        plain_sql = self._conn.select(
            self._grammar._plain_sql(table_name))[0]['sql']
        s = slice(plain_sql.find('(') + 1, plain_sql.rfind(')'))
        columns = plain_sql[s].split(', ')
        for column in columns:
            if 'AUTOINCREMENT' in column:
                column_name, *_ = column.partition(' ')
                result.append(column_name.strip('"'))
        return result

    def list_foreign_keys(self, table_name):
        """list foreign key from specified table"""
        sql = self._grammar._list_foreign_keys(table_name)
        result = self._conn.select(sql)
        foreign_keys = []
        for r in result:
            foreign_keys.append({
                'column': r['from'],
                'ref_key': r['to'],
                'to_table': r['table'],
                'on_update': r['on_update'],
                'on_delete': r['on_delete']
            })
        return foreign_keys

    def handle_primary_key(self, primary_key):
        return ['self.primary([{}])'.format(repr(primary_key[0].name))]

    def dump(self):
        table_names = list(self.list_tables())

        table_buffer = []
        for table in table_names:
            columns = self.list_columns(table)

            indexes = self.list_indexes(table)
            primary_key = list(filter(lambda column: column.pk, columns))
            foreign_keys = self.list_foreign_keys(table)
            statement_buffer = []

            statement_buffer.extend(self.handle_column(columns))
            statement_buffer.extend(self.handle_index(indexes))
            statement_buffer.extend(self.handle_foreign_key(foreign_keys))
            statement_buffer.extend(self.handle_primary_key(primary_key))

            table_buffer.append(self.table_tmpl.render(
                table_name=table,
                table_statement=statement_buffer
            ))

        output = self.schema_tmpl.render(
            tables_created=table_buffer,
            tables_droped=table_names
        )
        return output
