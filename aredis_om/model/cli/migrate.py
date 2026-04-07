import asyncio
import inspect

import click

from aredis_om.model.migrations.migrator import Migrator


async def _run_migrations(module: str):
    migrator = Migrator(module=module)
    await migrator.detect_migrations()

    if migrator.migrations:
        print("Pending migrations:")
        for migration in migrator.migrations:
            print(migration)

        if input("Run migrations? (y/n) ") == "y":
            return await migrator.run()
    return None


@click.command()
@click.option("--module", default="aredis_om")
def migrate(module: str):
    result = _run_migrations(module)
    if inspect.isawaitable(result):
        return asyncio.run(result)
    return result
