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
        t = TestValidateModel()
        t.name = 'test'
        self.assertIsNotNone(t.save())

    def test_validate_false(self):
        class TestNoneCaseModel(TestValidateModel):
            def validate_name(self):
                return False

        t = TestNoneCaseModel()
        t.name = 'test'
        with self.assertRaises(ValidationError):
            t.save()

    def test_validate_none(self):
        class TestNoneCaseModel(TestValidateModel):
            def validate(self, data):
                return None

        t = TestNoneCaseModel()
        t.name = 'test'
        with self.assertRaises(AssertionError):
            t.save()

    def test_no_validate(self):
        class TestNoValidateModel(Model):
            __table__ = 'users'

        t = TestNoValidateModel()
        t.name = 'test'
        self.assertIsNotNone(t.save())

    def test_validate_raise_error(self):
        class TestRaiseErrorModel(TestValidateModel):
            def validate(self, data):
                raise ValidationError('not valid')
                return data

        t = TestRaiseErrorModel()
        t.name = 'test'
        with self.assertRaises(ValidationError, msg='not valid'):
            t.save()


class TestValidateModel(Model):

    __table__ = 'users'

    def validate(self, data):
        return data

    def validate_name(self):
        return True


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
