# -*- coding: utf-8 -*-

from .. import OratorTestCase
from orator import Model
from orator.exceptions.orm import ValidationError
from orator.connections import SQLiteConnection
from orator.connectors import SQLiteConnector


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

    def test_validate_none(self):
        class TestNoneCaseModel(ValidateModel):
            def validate(self, data):
                return None

        t = TestNoneCaseModel()
        t.name = 'test'
        # with self.assertRaises(ValueError):
        self.assertIsNotNone(t.save())

    def test_validator_raise(self):
        t = ValidateModel()
        t.name = 'test3'
        with self.assertRaises(ValidationError):
            t.validate_name('test3')
        with self.assertRaises(ValueError):
            t.save()

    def test_validator_pop(self):

        t = ValidateModel()
        t.name = 'test4'
        t.is_valid()
        self.assertEqual(t.name, 'test5')
        self.assertIsNotNone(t.save())


class ValidateModel(Model):

    __table__ = 'users'

    def validate(self, data):
        if data['name'] == 'test1':
            raise ValidationError
        return data

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

        self._connection = SQLiteConnection(SQLiteConnector().connect({'database': ':memory:'}))

        return self._connection

    def get_default_connection(self):
        return 'default'

    def set_default_connection(self, name):
        pass
