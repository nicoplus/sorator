# -*- coding: utf-8 -*-

import os
import glob
import inspect
import textwrap
from flexmock import flexmock, flexmock_teardown
from .. import OratorTestCase
from orator.migrations import Migrator, DatabaseMigrationRepository, Migration
from orator.migrations.schema_dumper import dump
from orator import DatabaseManager
from orator.connections import Connection


class MigratorTestCase(OratorTestCase):

    def setUp(self):
        self.orig = inspect.getargspec
        inspect.getargspec = lambda fn: inspect.getfullargspec(fn)[:4]

    def tearDown(self):
        flexmock_teardown()
        inspect.getargspec = self.orig

    def test_migrations_are_run_up_when_outstanding_migrations_exist(self):
        resolver_mock = flexmock(DatabaseManager)
        resolver_mock.should_receive('connection').and_return({})
        resolver = flexmock(DatabaseManager({}))
        connection = flexmock()
        connection.should_receive('transaction').twice().and_return(connection)
        resolver.should_receive('connection').and_return(connection)

        migrator = flexmock(
            Migrator(
                flexmock(
                    DatabaseMigrationRepository(
                        resolver,
                        'migrations'
                    )
                ),
                resolver
            )
        )

        g = flexmock(glob)
        g.should_receive('glob').with_args(os.path.join(os.getcwd(), '[0-9]*_*.py')).and_return([
            os.path.join(os.getcwd(), '2_bar.py'),
            os.path.join(os.getcwd(), '1_foo.py'),
            os.path.join(os.getcwd(), '3_baz.py')
        ])

        migrator.get_repository().should_receive('get_ran').once().and_return(['1_foo'])
        migrator.get_repository().should_receive('get_next_batch_number').once().and_return(1)
        migrator.get_repository().should_receive('log').once().with_args('2_bar', 1)
        migrator.get_repository().should_receive('log').once().with_args('3_baz', 1)
        bar_mock = flexmock(MigrationStub())
        bar_mock.set_connection(connection)
        bar_mock.should_receive('up').once()
        baz_mock = flexmock(MigrationStub())
        baz_mock.set_connection(connection)
        baz_mock.should_receive('up').once()
        migrator.should_receive('_resolve').with_args(os.getcwd(), '2_bar').once().and_return(bar_mock)
        migrator.should_receive('_resolve').with_args(os.getcwd(), '3_baz').once().and_return(baz_mock)

        migrator.run(os.getcwd())

    def test_migrations_are_run_up_directly_if_transactional_is_false(self):
        resolver_mock = flexmock(DatabaseManager)
        resolver_mock.should_receive('connection').and_return({})
        resolver = flexmock(DatabaseManager({}))
        connection = flexmock()
        connection.should_receive('transaction').never()
        resolver.should_receive('connection').and_return(connection)

        migrator = flexmock(
            Migrator(
                flexmock(
                    DatabaseMigrationRepository(
                        resolver,
                        'migrations'
                    )
                ),
                resolver
            )
        )

        g = flexmock(glob)
        g.should_receive('glob').with_args(os.path.join(os.getcwd(), '[0-9]*_*.py')).and_return([
            os.path.join(os.getcwd(), '2_bar.py'),
            os.path.join(os.getcwd(), '1_foo.py'),
            os.path.join(os.getcwd(), '3_baz.py')
        ])

        migrator.get_repository().should_receive('get_ran').once().and_return(['1_foo'])
        migrator.get_repository().should_receive('get_next_batch_number').once().and_return(1)
        migrator.get_repository().should_receive('log').once().with_args('2_bar', 1)
        migrator.get_repository().should_receive('log').once().with_args('3_baz', 1)
        bar_mock = flexmock(MigrationStub())
        bar_mock.transactional = False
        bar_mock.set_connection(connection)
        bar_mock.should_receive('up').once()
        baz_mock = flexmock(MigrationStub())
        baz_mock.transactional = False
        baz_mock.set_connection(connection)
        baz_mock.should_receive('up').once()
        migrator.should_receive('_resolve').with_args(os.getcwd(), '2_bar').once().and_return(bar_mock)
        migrator.should_receive('_resolve').with_args(os.getcwd(), '3_baz').once().and_return(baz_mock)

        migrator.run(os.getcwd())

        resolver_mock = flexmock(DatabaseManager)
        resolver_mock.should_receive('connection').and_return({})
        resolver = flexmock(DatabaseManager({}))
        connection = flexmock(Connection(None))
        connection.should_receive('get_logged_queries').twice().and_return([])
        resolver.should_receive('connection').with_args(None).and_return(connection)

        migrator = flexmock(
            Migrator(
                flexmock(
                    DatabaseMigrationRepository(
                        resolver,
                        'migrations'
                    )
                ),
                resolver
            )
        )

        g = flexmock(glob)
        g.should_receive('glob').with_args(os.path.join(os.getcwd(), '[0-9]*_*.py')).and_return([
            os.path.join(os.getcwd(), '2_bar.py'),
            os.path.join(os.getcwd(), '1_foo.py'),
            os.path.join(os.getcwd(), '3_baz.py')
        ])

        migrator.get_repository().should_receive('get_ran').once().and_return(['1_foo'])
        migrator.get_repository().should_receive('get_next_batch_number').once().and_return(1)
        bar_mock = flexmock(MigrationStub())
        bar_mock.should_receive('get_connection').once().and_return(connection)
        bar_mock.should_receive('up').once()
        baz_mock = flexmock(MigrationStub())
        baz_mock.should_receive('get_connection').once().and_return(connection)
        baz_mock.should_receive('up').once()
        migrator.should_receive('_resolve').with_args(os.getcwd(), '2_bar').once().and_return(bar_mock)
        migrator.should_receive('_resolve').with_args(os.getcwd(), '3_baz').once().and_return(baz_mock)

        migrator.run(os.getcwd(), True)

    def test_nothing_is_done_when_no_migrations_outstanding(self):
        resolver_mock = flexmock(DatabaseManager)
        resolver_mock.should_receive('connection').and_return(None)
        resolver = flexmock(DatabaseManager({}))

        migrator = flexmock(
            Migrator(
                flexmock(
                    DatabaseMigrationRepository(
                        resolver,
                        'migrations'
                    )
                ),
                resolver
            )
        )

        g = flexmock(glob)
        g.should_receive('glob').with_args(os.path.join(os.getcwd(), '[0-9]*_*.py')).and_return([
            os.path.join(os.getcwd(), '1_foo.py')
        ])

        migrator.get_repository().should_receive('get_ran').once().and_return(['1_foo'])

        migrator.run(os.getcwd())

    def test_last_batch_of_migrations_can_be_rolled_back(self):
        resolver_mock = flexmock(DatabaseManager)
        resolver_mock.should_receive('connection').and_return({})
        resolver = flexmock(DatabaseManager({}))
        connection = flexmock()
        connection.should_receive('transaction').twice().and_return(connection)
        resolver.should_receive('connection').and_return(connection)

        migrator = flexmock(
            Migrator(
                flexmock(
                    DatabaseMigrationRepository(
                        resolver,
                        'migrations'
                    )
                ),
                resolver
            )
        )

        foo_migration = MigrationStub('foo')
        bar_migration = MigrationStub('bar')
        migrator.get_repository().should_receive('get_last').once().and_return([
            foo_migration,
            bar_migration
        ])

        bar_mock = flexmock(MigrationStub())
        bar_mock.set_connection(connection)
        bar_mock.should_receive('down').once()
        foo_mock = flexmock(MigrationStub())
        foo_mock.set_connection(connection)
        foo_mock.should_receive('down').once()
        migrator.should_receive('_resolve').with_args(os.getcwd(), 'bar').once().and_return(bar_mock)
        migrator.should_receive('_resolve').with_args(os.getcwd(), 'foo').once().and_return(foo_mock)

        migrator.get_repository().should_receive('delete').once().with_args(bar_migration)
        migrator.get_repository().should_receive('delete').once().with_args(foo_migration)

        migrator.rollback(os.getcwd())

    def test_last_batch_of_migrations_can_be_rolled_back_directly_if_transactional_is_false(self):
        resolver_mock = flexmock(DatabaseManager)
        resolver_mock.should_receive('connection').and_return({})
        resolver = flexmock(DatabaseManager({}))
        connection = flexmock()
        connection.should_receive('transaction').never()
        resolver.should_receive('connection').and_return(connection)

        migrator = flexmock(
            Migrator(
                flexmock(
                    DatabaseMigrationRepository(
                        resolver,
                        'migrations'
                    )
                ),
                resolver
            )
        )

        foo_migration = MigrationStub('foo')
        bar_migration = MigrationStub('bar')
        migrator.get_repository().should_receive('get_last').once().and_return([
            foo_migration,
            bar_migration
        ])

        bar_mock = flexmock(MigrationStub())
        bar_mock.transactional = False
        bar_mock.set_connection(connection)
        bar_mock.should_receive('down').once()
        foo_mock = flexmock(MigrationStub())
        foo_mock.transactional = False
        foo_mock.set_connection(connection)
        foo_mock.should_receive('down').once()
        migrator.should_receive('_resolve').with_args(os.getcwd(), 'bar').once().and_return(bar_mock)
        migrator.should_receive('_resolve').with_args(os.getcwd(), 'foo').once().and_return(foo_mock)

        migrator.get_repository().should_receive('delete').once().with_args(bar_migration)
        migrator.get_repository().should_receive('delete').once().with_args(foo_migration)

        migrator.rollback(os.getcwd())

    def test_rollback_migration_can_be_pretended(self):
        resolver_mock = flexmock(DatabaseManager)
        resolver_mock.should_receive('connection').and_return({})
        resolver = flexmock(DatabaseManager({}))
        connection = flexmock(Connection(None))
        connection.should_receive('get_logged_queries').twice().and_return([])
        resolver.should_receive('connection').with_args(None).and_return(connection)

        migrator = flexmock(
            Migrator(
                flexmock(
                    DatabaseMigrationRepository(
                        resolver,
                        'migrations'
                    )
                ),
                resolver
            )
        )

        foo_migration = flexmock(MigrationStub('foo'))
        foo_migration.should_receive('get_connection').and_return(connection)
        bar_migration = flexmock(MigrationStub('bar'))
        bar_migration.should_receive('get_connection').and_return(connection)
        migrator.get_repository().should_receive('get_last').once().and_return([
            foo_migration,
            bar_migration
        ])

        migrator.should_receive('_resolve').with_args(os.getcwd(), 'bar').once().and_return(bar_migration)
        migrator.should_receive('_resolve').with_args(os.getcwd(), 'foo').once().and_return(foo_migration)

        migrator.rollback(os.getcwd(), True)

        self.assertTrue(foo_migration.downed)
        self.assertFalse(foo_migration.upped)
        self.assertTrue(foo_migration.downed)
        self.assertFalse(foo_migration.upped)

    def test_nothing_is_rolled_back_when_nothing_in_repository(self):
        resolver = flexmock(DatabaseManager)
        resolver.should_receive('connection').and_return(None)

        migrator = flexmock(
            Migrator(
                flexmock(
                    DatabaseMigrationRepository(
                        resolver,
                        'migrations'
                    )
                ),
                resolver
            )
        )

        migrator.get_repository().should_receive('get_last').once().and_return([])

        migrator.rollback(os.getcwd())

    def test_mysql_schema_dump(self):
        from orator.schema.grammars import MySQLSchemaGrammar
        conn = flexmock()
        conn.should_receive('get_database_name').and_return('test_db')
        conn.should_receive('get_marker').and_return('?')
        conn.name = 'mysql'
        grammar = MySQLSchemaGrammar(conn)
        conn.should_receive('get_default_schema_grammar').and_return(grammar)
        conn.should_receive('select').with_args(grammar._list_tables()).and_return([
            {'table_name': 'migrations'},
            {'table_name': 'users'},
            {'table_name': 'user_info'},
            {'table_name': 'groups'}
        ])

        conn.should_receive('select').with_args(grammar._list_columns('groups')).and_return([
            {'precision': 10, 'unsigned': 'unsigned', 'name': 'id', 'ttype': 'int', 'extra': 'auto_increment', 'default': None, 'nullable': 'NO'},
            {'precision': 255, 'unsigned': None, 'name': 'name', 'ttype': 'varchar', 'extra': '', 'default': None, 'nullable': 'NO'},
            {'precision': 255, 'unsigned': None, 'name': 'category', 'ttype': 'varchar', 'extra': '', 'default': None, 'nullable': 'NO'},
            {'precision': 255, 'unsigned': None, 'name': 'bio', 'ttype': 'varchar', 'extra': '', 'default': None, 'nullable': 'YES'}
        ])

        conn.should_receive('select').with_args(grammar._list_columns('user_info')).and_return([
            {'precision': 10, 'unsigned': 'unsigned', 'name': 'id', 'ttype': 'int', 'extra': 'auto_increment', 'default': None, 'nullable': 'NO'},
            {'precision': 10, 'unsigned': 'unsigned', 'name': 'user_id', 'ttype': 'int', 'extra': '', 'default': None, 'nullable': 'NO'},
            {'precision': 5, 'unsigned': 'unsigned', 'name': 'age', 'ttype': 'smallint', 'extra': '', 'default': '18', 'nullable': 'NO'},
            {'precision': 255, 'unsigned': None, 'name': 'bio', 'ttype': 'varchar', 'extra': '', 'default': 'nothing', 'nullable': 'NO'},
            {'precision': 3, 'unsigned': None, 'name': 'is_stuff', 'ttype': 'tinyint', 'extra': '', 'default': None, 'nullable': 'NO'},
            {'precision': None, 'unsigned': None, 'name': 'created_at', 'ttype': 'timestamp', 'extra': '', 'default': 'CURRENT_TIMESTAMP(6)', 'nullable': 'NO'},
            {'precision': None, 'unsigned': None, 'name': 'updated_at', 'ttype': 'timestamp', 'extra': '', 'default': 'CURRENT_TIMESTAMP(6)', 'nullable': 'NO'}
        ])

        conn.should_receive('select').with_args(grammar._list_columns('users')).and_return([
            {'precision': 10, 'unsigned': 'unsigned', 'name': 'id', 'ttype': 'int', 'extra': 'auto_increment', 'default': None, 'nullable': 'NO'},
            {'precision': 128, 'unsigned': None, 'name': 'username', 'ttype': 'varchar', 'extra': '', 'default': None, 'nullable': 'NO'},
            {'precision': 128, 'unsigned': None, 'name': 'password', 'ttype': 'varchar', 'extra': '', 'default': None, 'nullable': 'NO'},
            {'precision': 255, 'unsigned': None, 'name': 'name', 'ttype': 'varchar', 'extra': '', 'default': None, 'nullable': 'NO'},
            {'precision': None, 'unsigned': None, 'name': 'created_at', 'ttype': 'timestamp', 'extra': '', 'default': 'CURRENT_TIMESTAMP(6)', 'nullable': 'NO'},
            {'precision': None, 'unsigned': None, 'name': 'updated_at', 'ttype': 'timestamp', 'extra': '', 'default': 'CURRENT_TIMESTAMP(6)', 'nullable': 'NO'}
        ])

        conn.should_receive('select').with_args(grammar._list_indexes('groups')).and_return([
            {'Table': 'groups', 'Column_name': 'id', 'Seq_in_index': 1, 'Packed': None, 'Comment': '', 'Null': '', 'Collation': 'A', 'Sub_part': None, 'Index_type': 'BTREE', 'Index_comment': '', 'Key_name': 'PRIMARY', 'Cardinality': 0, 'Non_unique': 0},
            {'Table': 'groups', 'Column_name': 'name', 'Seq_in_index': 1, 'Packed': None, 'Comment': '', 'Null': '', 'Collation': 'A', 'Sub_part': None, 'Index_type': 'BTREE', 'Index_comment': '', 'Key_name': 'groups_name_category_index', 'Cardinality': 0, 'Non_unique': 1},
            {'Table': 'groups', 'Column_name': 'category', 'Seq_in_index': 2, 'Packed': None, 'Comment': '', 'Null': '', 'Collation': 'A', 'Sub_part': None, 'Index_type': 'BTREE', 'Index_comment': '', 'Key_name': 'groups_name_category_index', 'Cardinality': 0, 'Non_unique': 1}
        ])
        conn.should_receive('select').with_args(grammar._list_indexes('user_info')).and_return([
            {'Table': 'user_info', 'Column_name': 'id', 'Seq_in_index': 1, 'Packed': None, 'Comment': '', 'Null': '', 'Collation': 'A', 'Sub_part': None, 'Index_type': 'BTREE', 'Index_comment': '', 'Key_name': 'PRIMARY', 'Cardinality': 0, 'Non_unique': 0},
            {'Table': 'user_info', 'Column_name': 'user_id', 'Seq_in_index': 1, 'Packed': None, 'Comment': '', 'Null': '', 'Collation': 'A', 'Sub_part': None, 'Index_type': 'BTREE', 'Index_comment': '', 'Key_name': 'user_info_user_id_foreign', 'Cardinality': 0, 'Non_unique': 1}
        ])
        conn.should_receive('select').with_args(grammar._list_indexes('users')).and_return(
            {'Table': 'users', 'Column_name': 'id', 'Seq_in_index': 1, 'Packed': None, 'Comment': '', 'Null': '', 'Collation': 'A', 'Sub_part': None, 'Index_type': 'BTREE', 'Index_comment': '', 'Key_name': 'PRIMARY', 'Cardinality': 0, 'Non_unique': 0},
            {'Table': 'users', 'Column_name': 'username', 'Seq_in_index': 1, 'Packed': None, 'Comment': '', 'Null': '', 'Collation': 'A', 'Sub_part': None, 'Index_type': 'BTREE', 'Index_comment': '', 'Key_name': 'users_username_unique', 'Cardinality': 0, 'Non_unique': 0}
        )

        conn.should_receive('select').with_args(grammar._list_foreign_keys('groups')).and_return([])

        conn.should_receive('select').with_args(grammar._list_foreign_keys('user_info')).and_return([
            {'column': 'user_id', 'on_delete': 'RESTRICT', 'name': 'user_info_user_id_foreign', 'on_update': 'RESTRICT', 'ref_key': 'id', 'to_table': 'users'}
        ])
        conn.should_receive('select').with_args(grammar._list_foreign_keys('users')).and_return([])

        output = dump(conn)
        correct_output = textwrap.dedent("""\
            from orator.migrations import Migration


            class InitDb(Migration):

                def up(self):
                    with self.schema.create('users') as table:
                        self.increments('id')
                        self.string('username', 128)
                        self.string('password', 128)
                        self.string('name', 255)
                        self.timestamp('created_at')
                        self.timestamp('updated_at')
                        self.primary(['id'], name=None)
                        self.unique(['username'], name='users_username_unique')

                    with self.schema.create('user_info') as table:
                        self.increments('id')
                        self.integer('user_id', 10).unsigned()
                        self.small_int('age', 5).unsigned().default(18)
                        self.string('bio', 255).default('nothing')
                        self.tiny_int('is_stuff', 3)
                        self.timestamp('created_at')
                        self.timestamp('updated_at')
                        self.primary(['id'], name=None)
                        self.index(['user_id'], name='user_info_user_id_foreign')
                        self.foreign('user_id').references('id').on('users')

                    with self.schema.create('groups') as table:
                        self.increments('id')
                        self.string('name', 255)
                        self.string('category', 255)
                        self.string('bio', 255).nullable()
                        self.primary(['id'], name=None)
                        self.index(['name', 'category'], name='groups_name_category_index')

                def down(self):
                    self.schema.drop('users')
                    self.schema.drop('user_info')
                    self.schema.drop('groups')
        """)
        assert correct_output == output

    def test_pgsql_schema_dump(self):
        from orator.schema.grammars import PostgresSchemaGrammar
        conn = flexmock()
        conn.should_receive('get_database_name').and_return('test_db')
        conn.should_receive('get_marker').and_return('?')
        conn.name = 'pgsql'
        grammar = PostgresSchemaGrammar(conn)
        conn.should_receive('get_default_schema_grammar').and_return(grammar)


        conn.should_receive('select').with_args(grammar._list_tables()).and_return([
            ['migrations'],
            ['users'],
            ['user_info'],
            ['groups']
        ])

        conn.should_receive('select').with_args(grammar._list_columns('users')).and_return([
            ['id', 'integer', 32, 'NO', "nextval('users_id_seq'::regclass)"],
            ['username', 'character varying', 128, 'NO', None],
            ['password', 'character varying', 128, 'NO', None],
            ['name', 'character varying', 255, 'NO', None],
            ['created_at', 'timestamp without time zone', None, 'NO', "('now'::text)::timestamp(6) with time zone"],
            ['updated_at', 'timestamp without time zone', None, 'NO', "('now'::text)::timestamp(6) with time zone"]
        ])

        conn.should_receive('select').with_args(grammar._list_columns('user_info')).and_return([
            ['id', 'integer', 32, 'NO', "nextval('user_info_id_seq'::regclass)"],
            ['user_id', 'integer', 32, 'NO', None],
            ['age', 'smallint', 16, 'NO', "'18'::smallint"],
            ['bio', 'character varying', 255, 'NO', "'nothing'::character varying"],
            ['is_stuff', 'boolean', None, 'NO', None],
            ['created_at', 'timestamp without time zone', None, 'NO', "('now'::text)::timestamp(6) with time zone"],
            ['updated_at', 'timestamp without time zone', None, 'NO', "('now'::text)::timestamp(6) with time zone"]
        ])

        conn.should_receive('select').with_args(grammar._list_columns('groups')).and_return([
            ['id', 'integer', 32, 'NO', "nextval('groups_id_seq'::regclass)"],
            ['name', 'character varying', 255, 'NO', None],
            ['category', 'character varying', 255, 'NO', None],
            ['bio', 'character varying', 255, 'YES', None]
        ])

        conn.should_receive('select').with_args(grammar._list_indexes('users')).and_return([
            ['users_pkey', 'CREATE UNIQUE INDEX users_pkey ON users USING btree (id)'],
            ['users_username_unique', 'CREATE UNIQUE INDEX users_username_unique ON users USING btree (username)']
        ])
        conn.should_receive('select').with_args(grammar._list_indexes('user_info')).and_return([
            ['user_info_pkey', 'CREATE UNIQUE INDEX user_info_pkey ON user_info USING btree (id)']
        ])
        conn.should_receive('select').with_args(grammar._list_indexes('groups')).and_return([
            ['groups_pkey', 'CREATE UNIQUE INDEX groups_pkey ON groups USING btree (id)'], ['groups_name_category_index', 'CREATE INDEX groups_name_category_index ON groups USING btree (name, category)']
        ])

        conn.should_receive('select').with_args(grammar._show_index('users_pkey')).and_return([
            ['id']
        ])
        conn.should_receive('select').with_args(grammar._show_index('users_username_unique')).and_return([
            ['username']
        ])
        conn.should_receive('select').with_args(grammar._show_index('user_info_pkey')).and_return([
            ['id']
        ])
        conn.should_receive('select').with_args(grammar._show_index('groups_pkey')).and_return([
            ['id']
        ])
        conn.should_receive('select').with_args(grammar._show_index('groups_name_category_index')).and_return([
            ['name'], ['category']
        ])

        conn.should_receive('select').with_args(grammar._list_foreign_keys('users')).and_return([])

        conn.should_receive('select').with_args(grammar._list_foreign_keys('user_info')).and_return([
            ['users', 'user_id', 'id', 'user_info_user_id_foreign', 'a', 'a']
        ])
        conn.should_receive('select').with_args(grammar._list_foreign_keys('groups')).and_return([])

        output = dump(conn)
        correct_output = textwrap.dedent("""\
            from orator.migrations import Migration


            class InitDb(Migration):
                def up(self):
                    with self.schema.create('users') as table:
                        self.increments('id')
                        self.string('username', 128)
                        self.string('password', 128)
                        self.string('name', 255)
                        self.timestamp('created_at')
                        self.timestamp('updated_at')
                        self.primary([('id',)], name=None)
                        self.unique([('username',)], name='users_username_unique')

                    with self.schema.create('user_info') as table:
                        self.increments('id')
                        self.integer('user_id', 32)
                        self.small_int('age', 16).default(18)
                        self.string('bio', 255).default('nothing')
                        self.boolean('is_stuff')
                        self.timestamp('created_at')
                        self.timestamp('updated_at')
                        self.primary([('id',)], name=None)
                        self.foreign('user_id').references(
                            'id').on('users').on_update('cascadea')

                    with self.schema.create('groups') as table:
                        self.increments('id')
                        self.string('name', 255)
                        self.string('category', 255)
                        self.string('bio', 255).nullable()
                        self.index([('name', 'category')],
                                   name='groups_name_category_index')
                        self.primary([('id',)], name=None)

                def down(self):
                    self.schema.drop('users')
                    self.schema.drop('user_info')
                    self.schema.drop('groups')
        """)
        assert correct_output == output

    def test_sqlite_schema_dump(self):
        from orator.schema.grammars import SQLiteSchemaGrammar
        conn = flexmock()
        conn.should_receive('get_database_name').and_return('test_db')
        conn.should_receive('get_marker').and_return('?')
        conn.name = 'sqlite'
        grammar = SQLiteSchemaGrammar(conn)
        conn.should_receive('get_default_schema_grammar').and_return(grammar)
        conn.should_receive('select').with_args(grammar._list_tables()).and_return([
            {'table_name': 'migrations'},
            {'table_name': 'users'},
            {'table_name': 'sqlite_sequence'},
            {'table_name': 'user_info'},
            {'table_name': 'groups'}
        ])

        conn.should_receive('select').with_args(grammar._list_columns('users')).and_return([
            {'cid': 0, 'pk': 1, 'type': 'INTEGER', 'notnull': 1, 'dflt_value': None, 'name': 'id'},
            {'cid': 1, 'pk': 0, 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'name': 'username'},
            {'cid': 2, 'pk': 0, 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'name': 'password'},
            {'cid': 3, 'pk': 0, 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'name': 'name'},
            {'cid': 4, 'pk': 0, 'type': 'DATETIME', 'notnull': 1, 'dflt_value': 'CURRENT_TIMESTAMP', 'name': 'created_at'},
            {'cid': 5, 'pk': 0, 'type': 'DATETIME', 'notnull': 1, 'dflt_value': 'CURRENT_TIMESTAMP', 'name': 'updated_at'}
        ])

        conn.should_receive('select').with_args(grammar._list_columns('user_info')).and_return([
            {'cid': 0, 'pk': 1, 'type': 'INTEGER', 'notnull': 1, 'dflt_value': None, 'name': 'id'},
            {'cid': 1, 'pk': 0, 'type': 'INTEGER', 'notnull': 1, 'dflt_value': None, 'name': 'user_id'},
            {'cid': 2, 'pk': 0, 'type': 'INTEGER', 'notnull': 1, 'dflt_value': "'18'", 'name': 'age'},
            {'cid': 3, 'pk': 0, 'type': 'TEXT', 'notnull': 1, 'dflt_value': "'nothing'", 'name': 'bio'},
            {'cid': 4, 'pk': 0, 'type': 'TINYINT', 'notnull': 1, 'dflt_value': None, 'name': 'is_stuff'},
            {'cid': 5, 'pk': 0, 'type': 'DATETIME', 'notnull': 1, 'dflt_value': 'CURRENT_TIMESTAMP', 'name': 'created_at'},
            {'cid': 6, 'pk': 0, 'type': 'DATETIME', 'notnull': 1, 'dflt_value': 'CURRENT_TIMESTAMP', 'name': 'updated_at'}
        ])

        conn.should_receive('select').with_args(grammar._list_columns('groups')).and_return([
            {'cid': 0, 'pk': 1, 'type': 'INTEGER', 'notnull': 1, 'dflt_value': None, 'name': 'id'},
            {'cid': 1, 'pk': 0, 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'name': 'name'},
            {'cid': 2, 'pk': 0, 'type': 'VARCHAR', 'notnull': 1, 'dflt_value': None, 'name': 'category'},
            {'cid': 3, 'pk': 0, 'type': 'VARCHAR', 'notnull': 0, 'dflt_value': None, 'name': 'bio'}
        ])

        conn.should_receive('select').with_args(grammar._plain_sql('users')).and_return([
            {'sql':"""CREATE TABLE "users" ("id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "username" VARCHAR NOT NULL, "password" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, "updated_at" DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL)"""}
        ])
        conn.should_receive('select').with_args(grammar._plain_sql('user_info')).and_return([
            {'sql':"""CREATE TABLE "user_info" ("id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "user_id" INTEGER NOT NULL, "age" INTEGER NOT NULL DEFAULT '18', "bio" TEXT NOT NULL DEFAULT 'nothing', "is_stuff" TINYINT NOT NULL, "created_at" DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, "updated_at" DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, FOREIGN KEY("user_id") REFERENCES "users"("id"))"""}
        ])
        conn.should_receive('select').with_args(grammar._plain_sql('groups')).and_return([
            {'sql':"""CREATE TABLE "groups" ("id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "name" VARCHAR NOT NULL, "category" VARCHAR NOT NULL, "bio" VARCHAR NULL)"""}
        ])


        conn.should_receive('select').with_args(grammar._list_indexes('users')).and_return([
            {'name': 'users_username_unique', 'seq': 0, 'origin': 'c', 'partial': 0, 'unique': 1}
        ])
        conn.should_receive('select').with_args(grammar._list_indexes('user_info')).and_return([])
        conn.should_receive('select').with_args(grammar._list_indexes('groups')).and_return([
            {'name': 'groups_name_category_index', 'seq': 0, 'origin': 'c', 'partial': 0, 'unique': 0}
        ])


        conn.should_receive('select').with_args(grammar._show_index('users_username_unique')).and_return([
            {'cid': 1, 'seqno': 0, 'name': 'username'}
        ])
        conn.should_receive('select').with_args(grammar._show_index('groups_name_category_index')).and_return([
            {'cid': 1, 'seqno': 0, 'name': 'name'},
            {'cid': 2, 'seqno': 1, 'name': 'category'}
        ])

        conn.should_receive('select').with_args(grammar._list_foreign_keys('users')).and_return([])

        conn.should_receive('select').with_args(grammar._list_foreign_keys('user_info')).and_return([
            {'from': 'user_id', 'seq': 0, 'match': 'NONE', 'to': 'id', 'on_delete': 'NO ACTION', 'id': 0, 'on_update': 'NO ACTION', 'table': 'users'}
        ])
        conn.should_receive('select').with_args(grammar._list_foreign_keys('groups')).and_return([])

        output = dump(conn)
        correct_output = textwrap.dedent("""\
            from orator.migrations import Migration


            class InitDb(Migration):

                def up(self):
                    with self.schema.create('users') as table:
                        self.increments('id')
                        self.string('username')
                        self.string('password')
                        self.string('name')
                        self.timestamp('created_at')
                        self.timestamp('updated_at')
                        self.unique(['username'], name='users_username_unique')
                        self.primary(['id'])

                    with self.schema.create('user_info') as table:
                        self.increments('id')
                        self.integer('user_id')
                        self.integer('age').default(18)
                        self.text('bio').default('nothing')
                        self.tiny_int('is_stuff')
                        self.timestamp('created_at')
                        self.timestamp('updated_at')
                        self.foreign('user_id').references('id').on('users')
                        self.primary(['id'])

                    with self.schema.create('groups') as table:
                        self.increments('id')
                        self.string('name')
                        self.string('category')
                        self.string('bio').nullable()
                        self.index(['name', 'category'], name='groups_name_category_index')
                        self.primary(['id'])

                def down(self):
                    self.schema.drop('users')
                    self.schema.drop('user_info')
                    self.schema.drop('groups')
        """)
        assert correct_output == output


class MigrationStub(Migration):

    def __init__(self, migration=None):
        self.migration = migration
        self.upped = False
        self.downed = False

    def up(self):
        self.upped = True

    def down(self):
        self.downed = True

    def __getitem__(self, item):
        return self.migration
