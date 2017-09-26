# -*- coding: utf-8 -*-

from . import DatabaseConnectionResolver, models
from .. import OratorTestCase
from orator import Model
from orator.exceptions.orm import ValidationError


class ModelValidateTestCase(OratorTestCase):

    @classmethod
    def setUpClass(cls):
        Model.set_connection_resolver(DatabaseConnectionResolver())

    @classmethod
    def tearDownClass(cls):
        Model.unset_connection_resolver()

    def connection(self):
        return Model.get_connection_resolver().connection()

    def schema(self):
        return self.connection().get_schema_builder()

    def setUp(self):
        connection = Model.get_connection_resolver().connection()
        schema = connection.get_schema_builder()
        with schema.create('users') as table:
            table.increments('id')
            table.string('name').unique()
            table.string('email').unique()
            table.boolean('admin').default(True)
            table.timestamps()

    def tearDown(self):
        self.schema().drop('users')

    def test_valid(self):
        t = User(name='john', email='admin@example.com')
        self.assertTrue(t.is_valid())
        self.assertEqual(t.name, 'john')
        self.assertEqual(t.cleaned_data['name'], 'john-cleaned')

    def test_validate_raise(self):
        t = User(name='john', email='xxx')
        with self.assertRaises(ValidationError):
            t.validate({'email': 'xxx'})
        with self.assertRaises(ValidationError):
            t.validate_name('invalid')
        with self.assertRaises(ValueError):
            t.save()
        self.assertIsNotNone(t.save({'run_validation': False}))


class User(models.User):
    __fillable__ = ['name', 'email']

    def validate(self, data):
        if '@' not in data['email']:
            raise ValidationError
        return data

    def validate_name(self, value):
        if value == 'invalid':
            raise ValidationError
        return value + '-cleaned'
