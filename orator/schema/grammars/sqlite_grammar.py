# -*- coding: utf-8 -*-

from .grammar import SchemaGrammar


class SQLiteSchemaGrammar(SchemaGrammar):

    _modifiers = ['unsigned', 'nullable', 'default', 'increment']

    _serials = ['big_integer', 'integer']

    def compile_rename_column(self, blueprint, command, connection):
        """
        Compile a rename column command.

        :param blueprint: The blueprint
        :type blueprint: Blueprint

        :param command: The command
        :type command: Fluent

        :param connection: The connection
        :type connection: orator.connections.Connection

        :rtype: list
        """
        sql = []
        # If foreign keys are on, we disable them
        foreign_keys = self._connection.select('PRAGMA foreign_keys')
        if foreign_keys:
            foreign_keys = bool(foreign_keys[0])
            if foreign_keys:
                sql.append('PRAGMA foreign_keys = OFF')

        sql += super().compile_rename_column(
            blueprint, command, connection)

        if foreign_keys:
            sql.append('PRAGMA foreign_keys = ON')

        return sql

    def compile_change(self, blueprint, command, connection):
        """
        Compile a change column command into a series of SQL statement.

        :param blueprint: The blueprint
        :type blueprint: orator.schema.Blueprint

        :param command: The command
        :type command: Fluent

        :param connection: The connection
        :type connection: orator.connections.Connection

        :rtype: list
        """
        sql = []
        # If foreign keys are on, we disable them
        foreign_keys = self._connection.select('PRAGMA foreign_keys')
        if foreign_keys:
            foreign_keys = bool(foreign_keys[0])
            if foreign_keys:
                sql.append('PRAGMA foreign_keys = OFF')

        sql += super(SQLiteSchemaGrammar,
                     self).compile_change(blueprint, command, connection)

        if foreign_keys:
            sql.append('PRAGMA foreign_keys = ON')

        return sql

    def compile_table_exists(self):
        """
        Compile the query to determine if a table exists

        :rtype: str
        """
        result = ("SELECT * FROM sqlite_master WHERE type = 'table' "
                  "AND name = %(marker)s" % {'marker': self.get_marker()})
        return result

    def compile_column_exists(self, table):
        """
        Compile the query to determine the list of columns.
        """
        return 'PRAGMA table_info(%s)' % table.replace('.', '__')

    def compile_create(self, blueprint, command, _):
        """
        Compile a create table command.
        """
        columns = ', '.join(self._get_columns(blueprint))

        sql = 'CREATE TABLE %s (%s' % (self.wrap_table(blueprint), columns)

        sql += self._add_foreign_keys(blueprint)

        sql += self._add_primary_keys(blueprint)

        return sql + ')'

    def _add_foreign_keys(self, blueprint):
        sql = ''

        foreigns = self._get_commands_by_name(blueprint, 'foreign')

        for foreign in foreigns:
            sql += self._get_foreign_key(foreign)

            if foreign.get('on_delete'):
                sql += ' ON DELETE %s' % foreign.on_delete

            if foreign.get('on_update'):
                sql += ' ON UPDATE %s' % foreign.on_delete

        return sql

    def _get_foreign_key(self, foreign):
        on = self.wrap_table(foreign.on)

        columns = self.columnize(foreign.columns)

        references = foreign.references
        if not isinstance(references, list):
            references = [references]

        on_columns = self.columnize(references)

        return ', FOREIGN KEY(%s) REFERENCES %s(%s)' % (
            columns, on, on_columns)

    def _add_primary_keys(self, blueprint):
        primary = self._get_command_by_name(blueprint, 'primary')

        if primary:
            columns = self.columnize(primary.columns)

            return ', PRIMARY KEY (%s)' % columns

        return ''

    def compile_add(self, blueprint, command, _):
        table = self.wrap_table(blueprint)

        columns = self.prefix_list('ADD COLUMN', self._get_columns(blueprint))

        statements = []

        for column in columns:
            statements.append('ALTER TABLE %s %s' % (table, column))

        return statements

    def compile_unique(self, blueprint, command, _):
        columns = self.columnize(command.columns)

        table = self.wrap_table(blueprint)

        return 'CREATE UNIQUE INDEX %s ON %s (%s)' % (
            command.index, table, columns)

    def compile_index(self, blueprint, command, _):
        columns = self.columnize(command.columns)

        table = self.wrap_table(blueprint)

        return 'CREATE INDEX %s ON %s (%s)' % (command.index, table, columns)

    def compile_foreign(self, blueprint, command, _):
        pass

    def compile_drop(self, blueprint, command, _):
        return 'DROP TABLE %s' % self.wrap_table(blueprint)

    def compile_drop_if_exists(self, blueprint, command, _):
        return 'DROP TABLE IF EXISTS %s' % self.wrap_table(blueprint)

    def compile_drop_column(self, blueprint, command, connection):
        schema = connection.get_schema_manager()

        table_diff = self._get_table_diff(blueprint, schema)

        for name in command.columns:
            column = connection.get_column(blueprint.get_table(), name)

            table_diff.removed_columns[name] = column

        return schema.get_database_platform().get_alter_table_sql(table_diff)

    def compile_drop_unique(self, blueprint, command, _):
        return 'DROP INDEX %s' % command.index

    def compile_drop_index(self, blueprint, command, _):
        return 'DROP INDEX %s' % command.index

    def compile_rename(self, blueprint, command, _):
        from_ = self.wrap_table(blueprint)

        return 'ALTER TABLE %s RENAME TO %s' % (
            from_, self.wrap_table(command.to))

    # why need comment in SQLite Schema?
    # Because multi orator type dump to same SQLite type
    # so meet a SQLite type, doesn't know the origin type
    def _type_char(self, column):
        return 'VARCHAR /*char(%%s,%s)*/' % column.length

    def _type_string(self, column):
        return 'VARCHAR /*string(%%s,%s)*/' % column.length

    def _type_text(self, column):
        return 'TEXT /*text(%s)*/'

    def _type_medium_text(self, column):
        return 'TEXT /*medium_text(%s)*/'

    def _type_long_text(self, column):
        return 'TEXT /*long_text(%s)*/'

    def _type_integer(self, column):
        return 'INTEGER /*integer(%s)*/'

    def _type_big_integer(self, column):
        return 'INTEGER /*big_integer(%s)*/'

    def _type_medium_integer(self, column):
        return 'INTEGER /*medium_integer(%s)*/'

    def _type_tiny_integer(self, column):
        return 'TINYINT /*tiny_integer(%s)*/'

    def _type_small_integer(self, column):
        return 'INTEGER /*small_integer(%s)*/'

    def _type_float(self, column):
        return 'FLOAT /*float(%s)*/'

    def _type_double(self, column):
        if column.total and column.places:
            return 'FLOAT /*double(%%s,%s,%s)*/' % (
                column.total, column.places)
        return 'FLOAT /*double(%s)*/'

    def _type_decimal(self, column):
        return 'NUMERIC /*DECIMAL(%%s,%s,%s)*/' % (
            column.total, column.places)

    def _type_boolean(self, column):
        return 'TINYINT /*boolean(%s)*/'

    def _type_enum(self, column):
        return 'VARCHAR /*enum(%%s,%s)*/' % column.allowed

    def _type_json(self, column):
        return 'TEXT /*json(%s)*/'

    def _type_date(self, column):
        return 'DATE /*date(%s)*/'

    def _type_datetime(self, column):
        return 'DATETIME /*datetime(%s)*/'

    def _type_time(self, column):
        return 'TIME /*time(%s)*/'

    def _type_timestamp(self, column):
        if column.use_current:
            return 'DATETIME /*timestamp(%s)*/ DEFAULT CURRENT_TIMESTAMP'

        return 'DATETIME /*timestamp(%s)*/'

    def _type_binary(self, column):
        return 'BLOB /*binary*/'

    def _modify_nullable(self, blueprint, column):
        if column.get('nullable'):
            return ' NULL'

        return ' NOT NULL'

    def _modify_unsigned(self, blueprint, column):
        # SQLite doesn't have unsigned
        # but the schema dumper need this info
        if column.get('unsigned', False):
            return ' /*unsigned*/'
        return ''

    def _modify_default(self, blueprint, column):
        if column.get('default') is not None:
            return ' DEFAULT %s' % self._get_default_value(column.default)

        return ''

    def _modify_increment(self, blueprint, column):
        if column.type in self._serials and column.auto_increment:
            return ' PRIMARY KEY AUTOINCREMENT'

        return ''

    def _get_dbal_column_type(self, type_):
        """
        Get the dbal column type.

        :param type_: The fluent type
        :type type_: str

        :rtype: str
        """
        type_ = type_.lower()

        if type_ == 'enum':
            return 'string'

        return super()._get_dbal_column_type(type_)

    def _list_tables(self):
        sql = """\
            SELECT name AS table_name
            FROM sqlite_master
            WHERE type="table"
        """
        return sql

    def _list_columns(self, table):
        sql = """\
            PRAGMA table_info('{}');
        """.format(table)
        return sql

    def _plain_sql(self, column):
        sql = """\
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table'
                AND name = '{}'
        """.format(column)
        return sql

    def _list_indexes(self, table):
        sql = """\
            PRAGMA index_list('{}')
        """.format(table)
        return sql

    def _show_index(self, index):
        sql = """\
            PRAGMA index_info('{}')
        """.format(index)
        return sql

    def _list_foreign_keys(self, table):
        sql = """\
            PRAGMA foreign_key_list('{}')
        """.format(table)
        return sql
