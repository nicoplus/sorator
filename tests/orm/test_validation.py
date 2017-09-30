# -*- coding: utf-8 -*-

from . import DatabaseConnectionResolver, models
from .. import OratorTestCase
from unittest import mock
from orator import Model
from orator.exceptions.orm import ValidationError
from orator.orm.validators import PresenceValidator, InclusionValidator, \
    ExclusionValidator, PatternValidator, NumericalityValidator, \
    LengthValidator, RangeValidator, UniquenessValidator


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
        self.assertIsNotNone(t.errors)
        self.assertIsNotNone(t.save({'run_validation': False}))


class ValidatorTestCase(OratorTestCase):

    def setUp(self):
        self.user = User(name='john', email='test@163.com')

    def test_LengthValidator(self):
        with self.assertRaises(ValidationError):
            validator = LengthValidator({'minimum': 10})
            validator(self.user, '123')

        with self.assertRaises(ValidationError):
            validator = LengthValidator({'maximum': 10})
            validator(self.user, '12345678901')

        validator = LengthValidator({'minimum': 1, 'maximum': 3})
        validator(self.user, '12')

        validator = LengthValidator({'in': (1, 3)})
        validator(self.user, '12')

        with self.assertRaises(ValidationError):
            validator = LengthValidator({'in': (1, 3)})
            validator(self.user, '123')

        validator = LengthValidator({'equal': 2})
        validator(self.user, '12')

    def test_PresenceValidator(self):
        with self.assertRaises(ValidationError):
            validator = PresenceValidator(True)
            validator(self.user, '')
        with self.assertRaises(ValidationError):
            validator = PresenceValidator(True)
            validator(self.user, ' ')
        with self.assertRaises(ValidationError):
            validator = PresenceValidator(True)
            validator(self.user, None)
        validator = PresenceValidator(True)
        validator(self.user, '234')

    def test_InclusionValidator(self):
        with self.assertRaises(ValidationError):
            validator = InclusionValidator([1, 2])
            validator(self.user, 3)
        validator = InclusionValidator([1, 2, 3])
        validator(self.user, 3)

    def test_ExclusionValidator(self):
        validator = ExclusionValidator([1, 2])
        validator(self.user, 3)
        with self.assertRaises(ValidationError):
            validator = ExclusionValidator([1, 2, 3])
            validator(self.user, 3)

    def test_PatternValidator(self):
        validator = PatternValidator(r'[+-]?\d+')
        with self.assertRaises(ValidationError):
            validator(self.user, '--123')
        validator(self.user, '-123')

    def test_NumericalityValidator(self):
        with self.assertRaises(ValidationError):
            validator = NumericalityValidator(True)
            validator(self.user, 'a123')
        validator(self.user, '123')

        validator = RangeValidator({'odd': True})
        validator(self.user, 1)

    def test_RangeValidator(self):
        with self.assertRaises(ValidationError):
            validator = RangeValidator({'gt': 2})
            validator(self.user, 1)
        validator = RangeValidator({'gt': 0})
        validator(self.user, 1)
        with self.assertRaises(ValidationError):
            validator = RangeValidator({'ge': 2})
            validator(self.user, 1)
        validator = RangeValidator({'ge': 2})
        validator(self.user, 2)

        with self.assertRaises(ValidationError):
            validator = RangeValidator({'lt': 2})
            validator(self.user, 2)
        validator = RangeValidator({'lt': 10})
        validator(self.user, 1)
        with self.assertRaises(ValidationError):
            validator = RangeValidator({'le': 2})
            validator(self.user, 4)
        validator = RangeValidator({'le': 2})
        validator(self.user, 2)

        validator = RangeValidator({'eq': 1})
        validator(self.user, 1)

    def test_UniquenessValidator(self):

        class QueryBuilder:
            def __init__(self, rv):
                self.rv = rv

            def limit(self, *args, **kwargs):
                return self

            def get(self, *args, **kwargs):
                return self.rv

        with mock.patch.object(User, 'where', return_value=QueryBuilder(True)):
            with self.assertRaises(ValidationError):
                validator = UniquenessValidator(True)
                validator.func_name = 'validate_name'
                validator(User(), '123')

        with mock.patch.object(User, 'where', return_value=QueryBuilder(False)):
            validator = UniquenessValidator(True)
            validator.func_name = 'validate_name'
            validator(User(), '123')
