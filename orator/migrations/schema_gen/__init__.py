"""
generate sorator schema builder from plain sql
"""

import re
import sqlparse
import autopep8
import textwrap
import sqlparse.tokens as TOKENS
from sqlparse.sql import TokenList
from jinja2 import Template as jinjaTemplate
from .fields import FieldFactory
from operator import itemgetter
from collections import defaultdict, OrderedDict


def dump(obj):
    """utility function for show dump result"""
    return obj.__dump__()


InfiniteDict = lambda: defaultdict(InfiniteDict)  # noqa


# use closure to hack the get_name method
TokenList.get_name = (lambda func:
                      (lambda self: func(self).strip('`'))
                      )(TokenList.get_name)

format_priority = ('increments', 'foreign', 'integer', 'string', 'timestamp')


class BadMatch(Exception):
    pass


class UnknownToken(Exception):
    pass


class Table:
    """SQL table representation"""

    dump_tmpl = jinjaTemplate(textwrap.dedent("""\
            with self.schema.create('{{ table_name }}') as table:
                {% for _, field in fields %}
                    {{- field.__dump__()}}
                {% endfor %}
                {% for primary_name, primary_field in primary  -%}
                    {%- if primary_name is none -%}
                        table.primary({{ primary_field }})
                    {%- else -%}
                        table.primary({{ primary_field }}, name='{{primary_name}}')
                    {%- endif %}
                {% endfor -%}
                {% for index_name, index_field in indexs -%}
                    table.index({{ index_field }}, name='{{ index_name }}')
                {% endfor -%}
                {% for unique_name, unique_field in uniques -%}
                    table.unique({{ unique_field }}, name='{{ unique_name }}') {# noqa #}
                {% endfor -%}
        """))

    def __init__(self, table_name):
        self._table_name = table_name
        self._fields = OrderedDict()
        self._indexs = OrderedDict()
        self._foreigns = OrderedDict()
        self._uniques = OrderedDict()
        self._primary = {None: ['id']}  # None meas anonymous

    def set_primary(self, columns):
        self._primary = {None: columns}

    def add_field(self, column):
        # self._fields.append(column)
        self._fields[column.name] = column

    def add_index(self, name, columns):
        self._indexs[name] = columns

    def add_unique(self, name, columns):
        self._uniques[name] = columns

    def add_foreign(self, name, local, foreign_table, foreign):
        self._foreigns[name] = (local, foreign_table, foreign)

    def __str__(self):
        string_buf = ['<Table object at {} field=['.format(hex(id(self)))]
        string_buf.append(','.join(map(str, self._fields.values())))
        string_buf.append('], ')

        if self._primary:
            string_buf.extend(['primary=', repr(self._primary)])
        if len(self._indexs):
            string_buf.extend(['index=', repr(self._indexs)])
        if len(self._uniques):
            string_buf.extend(['unique=', repr(self._uniques)])
        if len(self._foreigns):
            string_buf.extend(['foreign=', repr(self._foreigns)])

        string_buf.append(']>')
        return ''.join(string_buf)

    def __dump__(self):
        """dump to sorator schema builder synatax"""
        for foreign_name, (local_key, foreign_table, foreign_key) in \
                self._foreigns.items():
            assert len(foreign_key) == 1
            self._fields[local_key].upgrade_foreign(
                foreign_key=foreign_key, foreign_table=foreign_table)
        fields = sorted(self._fields.items(),
                        key=lambda item: float('inf')
                        if item[1].correspond not in format_priority else
                        format_priority.index(item[1].correspond))
        primary = sorted(self._primary.items(), key=itemgetter(1))
        indexs = sorted(self._indexs.items(), key=itemgetter(1))
        uniques = sorted(self._uniques.items(), key=itemgetter(1))
        return self.dump_tmpl.render(table_name=self._table_name,
                                     fields=fields,
                                     primary=primary,
                                     indexs=indexs,
                                     uniques=uniques)


class Lexer:
    """read the plain source code and return a token stream (an iterator)"""

    def __init__(self, sql):
        self._sql = sql
        self._tokens = iter(sqlparse.parse(sql)[0])

    def __next__(self):
        return next(self._tokens)

    def __len__(self):
        return self._tokens.__length_hint__()


def get_next(stream):
    """get next token from stream and skip Whitespace and Newline"""
    while True:
        token = next(stream)
        if not (token.ttype is TOKENS.Whitespace or
                token.ttype is TOKENS.Newline):
            # if token.is_whitespace:
            return token


def parse_field_syntax(stream, table, field_name):
    """parse sql field syntax"""
    token = get_next(stream)  # list type
    if token.ttype is None:
        field_type = token.get_name()
    else:
        field_type = token.value
    field = FieldFactory.new(field_name, field_type)
    if field_type in ('datetime', 'text',):
        field_type_arg = []
    else:
        field_type_arg = list(map(lambda f: f.value,
                                  token.get_parameters()))
    if field_type_arg:
        field.set_type_arg(field_type_arg)
    try:
        while True:
            field_constraint = get_next(stream)
            if field_constraint.ttype is TOKENS.Keyword:
                literal = field_constraint.value
                if literal == 'NOT NULL':
                    field.set_nullable(False)
                elif literal == 'DEFAULT':
                    field.set_default(get_next(stream).value)
                elif literal == 'AUTO_INCREMENT':
                    field.set_auto_incr(True)
                else:
                    raise UnknownToken(literal)
            elif field_constraint.ttype is TOKENS.Name.Builtin:
                literal = field_constraint.value
                if literal == 'unsigned':
                    field.set_unsigned(True)
                else:
                    raise UnknownToken(literal)
            else:
                raise UnknownToken(field_constraint)

    except StopIteration:
        pass
    table.add_field(field)


def parse_table_constraint(stream, table):
    """parse sql table constraint"""
    try:
        token = get_next(stream)
        ttype = token.ttype
        if ttype is TOKENS.Keyword:
            literal = token.value
            if literal == 'PRIMARY':
                # parse PRIMARY KEY
                assert get_next(stream).value == 'KEY'
                global debug
                debug = get_next(stream)
            elif literal == 'UNIQUE':
                # parse UNIQUE KEY
                assert get_next(stream).value == 'KEY'
                token = get_next(stream)
                key_name = token.get_name()
                columns = [x.get_name() for x in token.get_parameters()]
                table.add_unique(key_name, columns)
            elif literal == 'KEY':
                # parse INDEX
                token = get_next(stream)
                key_name = token.get_name()
                columns = [x.get_name() for x in token.get_parameters()]
                table.add_index(key_name, columns)
            elif literal == 'CONSTRAINT':
                key_name = get_next(stream).get_name()
                kind = get_next(stream).value
                if kind == 'FOREIGN':
                    assert get_next(stream).value == 'KEY'
                    local_key = get_next(stream).get_name()
                    assert get_next(stream).value == 'REFERENCES'
                    token = get_next(stream)
                    foreign_table = token.get_name()
                    foreign_key = [x.get_name()
                                   for x in token.get_parameters()]
                    table.add_foreign(key_name, local_key, foreign_table,
                                      foreign_key)
                else:
                    raise UnknownToken(kind)
    except StopIteration:
        pass


def parse_table_syntax(stream):
    """parse sql talbe syntax"""
    table_name = get_next(stream).get_name()
    table = Table(table_name)

    # parse all talbe field
    fields = get_next(stream).value.strip('()').split(',\n')
    constraint_re = re.compile(r'^\s*`(\w+)`')
    for field in fields:
        captured = constraint_re.match(field)
        if captured:
            field_name = captured.groups()[0]
            field = field.replace('`{}`'.format(field_name), '')
            parse_field_syntax(Lexer(field), table, field_name)
        else:
            parse_table_constraint(Lexer(field), table)
    return (table_name, table)


def parse_sql_syntax(plain_sql):
    """parse sql syntax"""
    token_stream = Lexer(plain_sql.strip())
    result = []
    while len(token_stream):
        token = get_next(token_stream)
        # DDL is shorten for Database Definition Language
        if token.ttype is TOKENS.DDL and token.value == 'CREATE':
            token = get_next(token_stream)
            if token.ttype is TOKENS.Keyword and token.value == 'TABLE':
                result.append(parse_table_syntax(token_stream))
            else:
                raise BadMatch()

    return result


def parse(buf_list):
    dump_tmpl = jinjaTemplate(textwrap.dedent("""\
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
    if not len(buf_list):
        return ''

    table_names, tables = zip(*parse_sql_syntax('\n'.join(buf_list)))
    return autopep8.fix_code(dump_tmpl.render(tables_created=map(dump, tables),
                                              tables_droped=table_names))
