# -*- coding: utf-8 -*-
import pytest
import sys

from .. import OratorTestCase
from orator import Model
from orator.orm.validators import *
from orator.exceptions.orm import ValidationError
from orator.connections import SQLiteConnection
from orator.connectors import SQLiteConnector

py2 = sys.version_info.major == 2

if py2:
    import mock
else:
    from unittest import mock


class ModelValidateTestCase(OratorTestCase):

    @classmethod
    def setUpClass(cls):
        Model.set_connection_resolver(DatabaseConnectionResolver())

    @classmethod
    def tearDownClass(cls):
        Model.unset_connection_resolver()

    def setUp(self):
        self.init_database()

    def test_valid(self):
        t = ValidateModel()
        t.name = 'test'
        self.assertIsNotNone(t.save())
        self.assertEqual(t.name, 'test')

    def test_no_validate(self):
        class TestNoValidateModel(Model):
            __table__ = 'users'

        t = TestNoValidateModel()
        t.name = 'test'
        self.assertIsNotNone(t.save())
        self.assertEqual(t.name, 'test')

    def test_validate_raise(self):

        t = ValidateModel()
        t.name = 'test1'
        with self.assertRaises(ValidationError):
            t.validate({'name': 'test1'})
        with self.assertRaises(ValueError):
            t.save()

    def test_validator_raise(self):
        t = ValidateModel()
        t.name = 'test3'
        with self.assertRaises(ValidationError):
            t.validate_name('test3')
        t.name = 'test'
        self.assertIsNotNone(t.save())

    def test_validator_change(self):

        t = ValidateModel()
        t.name = 'test4'
        t.is_valid()
        self.assertEqual(t.cleaned_data['name'], 'test5')
        self.assertIsNotNone(t.save())

    def test_validator_maxlen(self):
        t = ValidateModel()
        with self.assertRaises(ValidationError):
            t.validate_name('test1234567890')

    def test_not_run_validation(self):
        t = ValidateModel()
        t.name = 'test1'
        self.assertIsNotNone(t.save({'run_validation': False}))


class ValidateModel(Model):

    __table__ = 'users'

    def validate(self, data):
        if data['name'] == 'test1':
            raise ValidationError
        return data

    @validates(length={'maximum': 10})
    def validate_name(self, value):
        if value == 'test3':
            raise ValidationError
        if value == 'test4':
            return 'test5'
        return value


class DatabaseConnectionResolver(object):

    _connection = None

    def connection(self, name=None):
        if self._connection:
            return self._connection

        self._connection = SQLiteConnection(SQLiteConnector().connect({
            'database': ':memory:'}))

        return self._connection

    def get_default_connection(self):
        return 'default'

    def set_default_connection(self, name):
        pass


def test_LengthValidator():
    m = ValidateModel()
    with pytest.raises(ValidationError):
        v = LengthValidator({'minimum': 10})
        v(m, '123')

    with pytest.raises(ValidationError):
        v = LengthValidator({'maximum': 10})
        v(m, '12345678901')

    v = LengthValidator({'minimum': 1, 'maximum': 3})
    v(m, '12')

    v = LengthValidator({'in': (1, 3)})
    v(m, '12')

    with pytest.raises(ValidationError):
        v = LengthValidator({'in': (1, 3)})
        v(m, '123')

    v = LengthValidator({'equal': 2})
    v(m, '12')


def test_PresenceValidator():
    m = ValidateModel()
    with pytest.raises(ValidationError):
        v = PresenceValidator(True)
        v(m, '')
    with pytest.raises(ValidationError):
        v = PresenceValidator(True)
        v(m, ' ')
    with pytest.raises(ValidationError):
        v = PresenceValidator(True)
        v(m, b'  ')
    with pytest.raises(ValidationError):
        v = PresenceValidator(True)
        v(m, None)
    v = PresenceValidator(True)
    v(m, '234')


def test_InclusionValidator():
    m = ValidateModel()
    with pytest.raises(ValidationError):
        v = InclusionValidator([1, 2])
        v(m, 3)
    v = InclusionValidator([1, 2, 3])
    v(m, 3)


def test_ExclusionValidator():
    m = ValidateModel()
    v = ExclusionValidator([1, 2])
    v(m, 3)
    with pytest.raises(ValidationError):
        v = ExclusionValidator([1, 2, 3])
        v(m, 3)


def test_PatternValidator():
    m = ValidateModel()
    v = PatternValidator(r'[+-]?\d+')
    with pytest.raises(ValidationError):
        v(m, '--123')
    v(m, '-123')


def test_NumericalityValidator():
    m = ValidateModel()
    with pytest.raises(ValidationError):
        v = NumericalityValidator(True)
        v(m, 'a123')
    v(m, '123')


def test_RangeValidator():
    m = ValidateModel()
    with pytest.raises(ValidationError):
        v = RangeValidator({'greater_than': 2})
        v(m, 1)
    v = RangeValidator({'greater_than': 0})
    v(m, 1)
    with pytest.raises(ValidationError):
        v = RangeValidator({'greater_than_or_equal_to': 2})
        v(m, 1)
    v = RangeValidator({'greater_than_or_equal_to': 2})
    v(m, 2)

    with pytest.raises(ValidationError):
        v = RangeValidator({'less_than': 2})
        v(m, 2)
    v = RangeValidator({'less_than': 10})
    v(m, 1)
    with pytest.raises(ValidationError):
        v = RangeValidator({'less_than_or_equal_to': 2})
        v(m, 4)
    v = RangeValidator({'less_than_or_equal_to': 2})
    v(m, 2)

    v = RangeValidator({'equal_to': 1})
    v(m, 1)

    v = RangeValidator({'odd': True})
    v(m, 1)


def test_UniquenessValidator():
    with pytest.raises(ValidationError):
        fake_obj = mock.MagicMock()
        fake_obj.where().count.return_value = 1
        v = UniquenessValidator(True)
        v.func_name = 'validate_name'
        v(fake_obj, '123')

    fake_obj = mock.MagicMock()
    fake_obj.where().count.return_value = 0
    v = UniquenessValidator(True)
    v.func_name = 'validate_name'
    v(fake_obj, '123')
