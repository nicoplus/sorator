import textwrap
from jinja2 import Template as jinjaTemplate


class Dumper:

    table_tmpl = jinjaTemplate(textwrap.dedent("""\
        with self.schema.create('{{ table_name }}') as table:
            {% for statement in table_statement %}
                {{- statement }}
            {% endfor %}
    """))

    schema_tmpl = jinjaTemplate(textwrap.dedent("""\
    from orator.migrations import Migration


    class InitDb(Migration):

        def up(self):
            {% for table in tables_created %}
                {{- table | indent(8) }}
            {% endfor %}

        def down(self):
            {% for table in tables_droped -%}
                self.schema.drop('{{ table }}')
            {% endfor %}
    """))

    def __init__(self, conn, grammar, db_name):
        """
        @param grammar: grammar instance
        @param da_name: str
        """
        self._conn = conn
        self._grammar = grammar
        self._db_name = db_name

    def handle_column(self, columns):

        raise NotImplementedError()

    def handle_index(self, indexes):

        raise NotImplementedError()

    def handle_foreign_key(self, foreign_keys):

        raise NotImplementedError()

    def dump(self):
        table_names = list(self.list_tables())

        table_buffer = []
        for table in table_names:
            columns = self.list_columns(table)
            indexes = self.list_indexes(table)
            foreign_keys = self.list_foreign_keys(table)
            statement_buffer = []

            statement_buffer.extend(self.handle_column(columns))
            statement_buffer.extend(self.handle_index(indexes))
            statement_buffer.extend(self.handle_foreign_key(foreign_keys))

            table_buffer.append(self.table_tmpl.render(
                table_name=table,
                table_statement=statement_buffer
            ))

        output = self.schema_tmpl.render(
            tables_created=table_buffer,
            tables_droped=table_names
        )
        return output

    def list_tables(self):

        raise NotImplementedError()

    def list_columns(self, table_name):

        raise NotImplementedError()

    def list_indexes(self, table_name):

        raise NotImplementedError()

    def list_foreign_keys(self, table_name):

        raise NotImplementedError()
