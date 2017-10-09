# -*- coding: utf-8 -*-

from cleo import Application
from .. import __version__


# Migrations
from .migrations import (
    InstallCommand, MigrateCommand,
    MigrateMakeCommand, RollbackCommand,
    StatusCommand, ResetCommand, RefreshCommand
)

# Seeds
from .seeds import SeedersMakeCommand, SeedCommand

# Models
from .models import ModelMakeCommand

application = Application('Orator', __version__, complete=True)

application.add(InstallCommand())
application.add(MigrateCommand())
application.add(MigrateMakeCommand())
application.add(RollbackCommand())
application.add(StatusCommand())
application.add(ResetCommand())
application.add(RefreshCommand())
application.add(SeedersMakeCommand())
application.add(SeedCommand())
application.add(ModelMakeCommand())
