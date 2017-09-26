# -*- coding: utf-8 -*-
import os
from unittest import TestCase
from orator.database_manager import DatabaseManager
from .orm.models import Model

from unittest import mock


class OratorTestCase(TestCase):

    def tearDown(self):
        if hasattr(self, 'local_database'):
            os.remove(self.local_database)

    def init_database(self):
        self.local_database = '/tmp/orator_test_database.db'

        if os.path.exists(self.local_database):
            os.remove(self.local_database)

        self.manager = DatabaseManager({
            'default': 'sqlite',
            'sqlite': {
                'driver': 'sqlite',
                'database': self.local_database
            }
        })

        with self.manager.transaction():
            try:
                self.manager.statement(
                    'CREATE TABLE `users` ('
                    'id INTEGER PRIMARY KEY NOT NULL, '
                    'name CHAR(50) NOT NULL, '
                    'created_at DATETIME DEFAULT CURRENT_TIMESTAMP, '
                    'updated_at DATETIME DEFAULT CURRENT_TIMESTAMP'
                    ')'
                )
            except Exception:
                pass

        Model.set_connection_resolver(self.manager)

        self.manager.disconnect()
