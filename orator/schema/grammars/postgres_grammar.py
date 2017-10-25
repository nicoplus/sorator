# -*- coding: utf-8 -*-

from .grammar import SchemaGrammar


class PostgresSchemaGrammar(SchemaGrammar):

    _modifiers = ['increment', 'nullable', 'default']

    _serials = ['big_integer', 'integer',
                'medium_integer', 'small_integer', 'tiny_integer']

    marker = '%s'

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
        table = self.get_table_prefix() + blueprint.get_table()

        column = self.wrap(command.from_)

        return 'ALTER TABLE %s RENAME COLUMN %s TO %s'\
               % (table, column, self.wrap(command.to))

    def compile_table_exists(self):
        """
        Compile the query to determine if a table exists

        :rtype: str
        """
        return 'SELECT * ' \
               'FROM information_schema.tables ' \
               'WHERE table_name = %(marker)s' \
               % {'marker': self.get_marker()}

    def compile_column_exists(self, table):
        """
        Compile the query to determine the list of columns.
        """
        return 'SELECT column_name ' \
               'FROM information_schema.columns ' \
               'WHERE table_name = \'%s\'' % table

    def compile_create(self, blueprint, command, _):
        """
        Compile a create table command.
        """
        columns = ', '.join(self._get_columns(blueprint))

        return 'CREATE TABLE %s (%s)' % (self.wrap_table(blueprint), columns)

    def compile_add(self, blueprint, command, _):
        table = self.wrap_table(blueprint)

        columns = self.prefix_list('ADD COLUMN', self._get_columns(blueprint))

        return 'ALTER TABLE %s %s' % (table, ', '.join(columns))

    def compile_primary(self, blueprint, command, _):
        columns = self.columnize(command.columns)

        return 'ALTER TABLE %s ADD PRIMARY KEY (%s)'\
               % (self.wrap_table(blueprint), columns)

    def compile_unique(self, blueprint, command, _):
        columns = self.columnize(command.columns)

        table = self.wrap_table(blueprint)

        return 'ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)'\
               % (table, command.index, columns)

    def compile_index(self, blueprint, command, _):
        columns = self.columnize(command.columns)

        table = self.wrap_table(blueprint)

        return 'CREATE INDEX %s ON %s (%s)' % (command.index, table, columns)

    def compile_drop(self, blueprint, command, _):
        return 'DROP TABLE %s' % self.wrap_table(blueprint)

    def compile_drop_if_exists(self, blueprint, command, _):
        return 'DROP TABLE IF EXISTS %s' % self.wrap_table(blueprint)

    def compile_drop_column(self, blueprint, command, connection):
        columns = self.prefix_list(
            'DROP COLUMN', self.wrap_list(command.columns))

        table = self.wrap_table(blueprint)

        return 'ALTER TABLE %s %s' % (table, ', '.join(columns))

    def compile_drop_primary(self, blueprint, command, _):
        table = blueprint.get_table()

        return 'ALTER TABLE %s DROP CONSTRAINT %s_pkey'\
               % (self.wrap_table(blueprint), table)

    def compile_drop_unique(self, blueprint, command, _):
        table = self.wrap_table(blueprint)

        return 'ALTER TABLE %s DROP CONSTRAINT %s' % (table, command.index)

    def compile_drop_index(self, blueprint, command, _):
        return 'DROP INDEX %s' % command.index

    def compile_drop_foreign(self, blueprint, command, _):
        table = self.wrap_table(blueprint)

        return 'ALTER TABLE %s DROP CONSTRAINT %s' % (table, command.index)

    def compile_rename(self, blueprint, command, _):
        from_ = self.wrap_table(blueprint)

        return 'ALTER TABLE %s RENAME TO %s' % (
            from_, self.wrap_table(command.to))

    def _type_char(self, column):
        return "CHAR(%s)" % column.length

    def _type_string(self, column):
        return "VARCHAR(%s)" % column.length

    def _type_text(self, column):
        return 'TEXT'

    def _type_medium_text(self, column):
        return 'TEXT'

    def _type_long_text(self, column):
        return 'TEXT'

    def _type_integer(self, column):
        return 'SERIAL' if column.auto_increment else 'INTEGER'

    def _type_big_integer(self, column):
        return 'BIGSERIAL' if column.auto_increment else 'BIGINT'

    def _type_medium_integer(self, column):
        return 'SERIAL' if column.auto_increment else 'INTEGER'

    def _type_tiny_integer(self, column):
        return 'SMALLSERIAL' if column.auto_increment else 'SMALLINT'

    def _type_small_integer(self, column):
        return 'SMALLSERIAL' if column.auto_increment else 'SMALLINT'

    def _type_float(self, column):
        return self._type_double(column)

    def _type_double(self, column):
        return 'DOUBLE PRECISION'

    def _type_decimal(self, column):
        return 'DECIMAL(%s, %s)' % (column.total, column.places)

    def _type_boolean(self, column):
        return 'BOOLEAN'

    def _type_enum(self, column):
        allowed = list(map(lambda a: "'%s'" % a, column.allowed))

        return 'VARCHAR(255) CHECK ("%s" IN (%s))' % (
            column.name, ', '.join(allowed))

    def _type_json(self, column):
        return 'JSON'

    def _type_date(self, column):
        return 'DATE'

    def _type_datetime(self, column):
        return 'TIMESTAMP(6) WITHOUT TIME ZONE'

    def _type_time(self, column):
        return 'TIME(6) WITHOUT TIME ZONE'

    def _type_timestamp(self, column):
        if column.use_current:
            return ('TIMESTAMP(6) WITHOUT TIME ZONE '
                    'DEFAULT CURRENT_TIMESTAMP(6)')

        return 'TIMESTAMP(6) WITHOUT TIME ZONE'

    def _type_binary(self, column):
        return 'BYTEA'

    def _modify_nullable(self, blueprint, column):
        if column.get('nullable'):
            return ' NULL'

        return ' NOT NULL'

    def _modify_default(self, blueprint, column):
        if column.get('default') is not None:
            return ' DEFAULT %s' % self._get_default_value(column.default)

        return ''

    def _modify_increment(self, blueprint, column):
        if column.type in self._serials and column.auto_increment:
            return ' PRIMARY KEY'

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
            SELECT c.relname AS "table_name"
            FROM pg_catalog.pg_class c
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r','')
                  AND n.nspname <> 'pg_catalog'
                  AND n.nspname <> 'information_schema'
                  AND n.nspname !~ '^pg_toast'
                  AND pg_catalog.pg_table_is_visible(c.oid)
        """
        return sql

    def _list_columns(self, table_name):
        sql = """\
            SELECT column_name AS "name",
                   data_type AS "ttype",
                   COALESCE(numeric_precision, character_maximum_length) \
                        AS "precision",
                   is_nullable AS "nullable",
                   column_default AS "default"
            FROM information_schema.columns
            WHERE table_name = '{}'
        """.format(table_name)
        return sql

    def _list_indexes(self, table_name):
        sql = """\
            SELECT
                i.relname AS "index_name",
                array_to_string(array_agg(a.attname), ', ') AS "column_names",
                pc.contype AS "ttype"
            FROM
                pg_class t,
                pg_class i,
                pg_index ix,
                pg_attribute a,
                pg_constraint pc
            WHERE
                t.oid = ix.indrelid
                and i.oid = ix.indexrelid
                and a.attrelid = t.oid
                and a.attnum = ANY(ix.indkey)
                and t.relkind = 'r'
                and t.relname like '{}'
                and pc.conname = i.relname
            GROUP BY
                t.relname,
                i.relname,
                pc.contype
            ORDER BY
                t.relname,
                i.relname;
        """.format(table_name)
        return sql

    def _list_foreign_keys(self, table_name):
        sql = """\
            SELECT t2.oid::regclass::text AS "to_table",
                   a1.attname AS "column",
                   a2.attname AS "primary_key",
                   c.conname AS "name",
                   c.confupdtype AS "on_update",
                   c.confdeltype AS "on_delete"
            FROM pg_constraint c
            JOIN pg_class t1 ON c.conrelid = t1.oid
            JOIN pg_class t2 ON c.confrelid = t2.oid
            JOIN pg_attribute a1 ON a1.attnum = c.conkey[1]
                AND a1.attrelid = t1.oid
            JOIN pg_attribute a2 ON a2.attnum = c.confkey[1]
                AND a2.attrelid = t2.oid
            JOIN pg_namespace t3 ON c.connamespace = t3.oid
            WHERE c.contype = 'f'
                AND t1.relname = '{}'
            ORDER BY c.conname
        """.format(table_name)
        return sql
