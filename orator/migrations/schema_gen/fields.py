"""
list all mysql fields and their correspond type in sorator
"""

import types


class Field:
    """SQL Field implemention"""

    def __init__(self, name, ttype):
        self._name = name
        self._type = ttype
        self._type_arg = []
        self._nullable = True
        self._auto_incr = False
        self._auto_incr_begin = 0
        self._unsigned = False
        self._default_value = None

    @property
    def name(self):
        return self._name

    @property
    def ttype(self):
        return self._type

    @property
    def correspond(self):
        return self.__correspond__

    def set_type_arg(self, type_arg):
        self._type_arg = type_arg

    def set_nullable(self, boolean):
        self._nullable = boolean

    def set_auto_incr(self, boolean):
        self._auto_incr = boolean

    def set_auto_incr_begin(self, value):
        self._auto_incr_begin = value

    def set_unsigned(self, boolean):
        self._unsigned = boolean

    def set_default(self, value):
        self._default_value = value

    def __str__(self):
        string_buf = ['<{} object at {} "'.format(self.__class__.__name__,
                                                  hex(id(self)))]
        string_buf.append('{} {}'.format(self._name, self._type))
        if len(self._type_arg):
            string_buf.append(' ({})'.format(*self._type_arg))
        # others
        if self._unsigned:
            string_buf.append(' unsigned')

        if self._default_value is not None:
            string_buf.append(' DEFAULT {}'.format(self._default_value))
        elif not self._nullable:
            string_buf.append(' NOT NULL')

        if self._auto_incr:
            string_buf.append(' AUTO_INCREMENT')

        string_buf.append('">')
        return ''.join(string_buf)

    def _dump_main(self):
        ttype = self.__correspond__
        args = []
        args.append(self._name)
        args.extend(
            map(lambda x: int(x) if x.isdigit() else x, self._type_arg))
        self._string_buf.append(
            '.{}({})'.format(ttype, ', '.join(map(repr, args))))

    def _dump_other(self):
        if self._unsigned:
            self._string_buf.append('.unsigned()')
        default_value = self._default_value
        if default_value and default_value != 'NULL':
            default_value = default_value.strip("'")
            if default_value.isdigit():
                default_value = int(default_value)
            self._string_buf.append('.default({})'.format(repr(default_value)))
        elif self._nullable:
            self._string_buf.append('.nullable()')

    def __dump__(self):
        """dump to sorator syntax """
        self._string_buf = ['table']

        self._dump_main()

        # handle constraint
        self._dump_other()
        plain_text = ''.join(self._string_buf)
        delattr(self, '_string_buf')
        return plain_text


class INTEGERField(Field):
    __correspond__ = 'integer'

    def set_auto_incr(self, boolean):
        self.upgrade_increments()
        super().set_auto_incr(boolean)

    def _dump_main(self):
        self._string_buf.append(".{}('{}')".format(
            self.__correspond__, self._name))

    def upgrade_foreign(self, foreign_key, foreign_table):
        self.__correspond__ = 'foreign'

        def new_dump_main(self):
            assert len(foreign_key) == 1
            self._string_buf.append(
                ".{}('{}').references('{}').on('{}')".format(
                    self.__correspond__, self._name,
                    foreign_key[0], foreign_table,))
        self._dump_main = types.MethodType(new_dump_main, self)

    def upgrade_increments(self):
        self.__correspond__ = 'increments'


class BIGINTField(Field):
    __correspond__ = 'big_integer'

    def set_auto_incr(self, boolean):
        self.upgrade_increments()
        super().set_auto_incr(boolean)

    def _dump_main(self):
        self._string_buf.append(
            ".{}('{}')".format(self.__correspond__, self._name))

    def upgrade_increments(self):
        self.__correspond__ = 'increments'


class BLOBField(Field):
    __correspond__ = 'binary'


class BOOLEANField(Field):
    __correspond__ = 'boolean'


class CHARField(Field):
    __correspond__ = 'char'


class DATEField(Field):
    __correspond__ = 'date'


class DATETIMEField(Field):
    __correspond__ = 'datetime'


class DECIMALField(Field):
    __correspond__ = 'decimal'


class DOUBLEField(Field):
    __correspond__ = 'double'


class ENUMField(Field):
    __correspond__ = 'enum'


class FLOATField(Field):
    __correspond__ = 'float'


class JSONField(Field):
    __correspond__ = 'json'


class LONGTEXTField(Field):
    __correspond__ = 'long_text'


class MEDIUMINTField(Field):
    __correspond__ = 'medium_int'


class MEDIUMTEXTField(Field):
    __correspond__ = 'medium_text'


class SMALLINTField(Field):
    __correspond__ = 'small_integer'


class TEXTField(Field):
    __correspond__ = 'text'


class TIMEField(Field):
    __correspond__ = 'time'


class TIMESTAMPField(Field):
    __correspond__ = 'timestamp'

    def _dump_main(self):
        ttype = self.__correspond__
        self._string_buf.append('.{}({})'.format(ttype, repr(self._name)))

    def _dump_other(self):
        super()._dump_other()
        # default_value
        self._string_buf.pop()


class VARCHARField(Field):
    __correspond__ = 'string'


class TINYINTField(Field):
    __correspond__ = 'tiny_int'

    def _dump_main(self):
        if len(self._type_arg) == 1 and self._type_arg[0]:
            self.upgrade_boolean()
            self._default_value = None
        self._string_buf.append(
            ".{}('{}')".format(self.__correspond__, self._name))

    def upgrade_boolean(self):
        self.__correspond__ = 'boolean'
        self._type_arg = []


class FieldFactory:

    mapping = {
        'BIGINT': BIGINTField,
        'BLOB': BLOBField,
        'BOOLEAN': BOOLEANField,
        'CHAR': CHARField,
        'DATE': DATEField,
        'DATETIME': DATETIMEField,
        'DECIMAL': DECIMALField,
        'DOUBLE': DOUBLEField,
        'ENUM': ENUMField,
        'FLOAT': FLOATField,
        'INT': INTEGERField,
        'JSON': JSONField,
        'LONGTEXT': LONGTEXTField,
        'MEDIUMINT': MEDIUMINTField,
        'MEDIUMTEXT': MEDIUMTEXTField,
        'SMALLINT': SMALLINTField,
        'TEXT': TEXTField,
        'TIME': TIMEField,
        'TINYINT': TINYINTField,
        'TIMESTAMP': TIMESTAMPField,
        'VARCHAR': VARCHARField
    }

    @classmethod
    def new(cls, name, ttype):
        return cls.mapping.get(ttype.upper())(name, ttype)


__all__ = ['FieldFactory']
