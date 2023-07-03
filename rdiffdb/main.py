#!/usr/bin/env python

from pathlib import Path
import time
from typing import Annotated
import typer
from settings import hosts, containersettings
from rich.progress import Progress, SpinnerColumn, TextColumn

app = typer.Typer()


@app.command()
def list_hosts():
    print(hosts.keys())


@app.command()
def backup_db(host: Annotated[str, typer.Option(prompt=True)]):
    h = hosts.get(host)
    if not host:
        raise KeyError("Host not found")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
    ) as progress:
        task = None
        for result in h.run_backup_db():
            if isinstance(result, str):
                task = progress.add_task(description=f"{result}", total=None)
            elif isinstance(result, bool) and result is True and task:
                progress.update(task_id=task, completed=1)

        # Run `rdiff-backup`
        task = progress.add_task(description=f"Rdiff-backup to {h.paths.destination}", total=None)
        h.rdiff_backup()
        progress.update(task_id=task, completed=1)

@app.command()
def list_backups(host: Annotated[str, typer.Option(prompt=True)]):
    h = hosts.get(host)
    h.list_backups()

@app.command()
def build_container(host: Annotated[str, typer.Option(prompt=True)]):
    config = hosts.get(host)
    containerconfig = containersettings.get(host)

    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
        ) as progress:

        restore = progress.add_task(description=f"Temporary restore-as-of-now from {config.paths.destination} to {config.paths.local_backup}", total=None)
        copy_to_container = progress.add_task(description=f"Build container {containerconfig.container_name}", total=None)

        container_restore = progress.add_task("Wait 5 seconds then Restore the database", total=None)

        config.restore_as_of_now()
        progress.update(restore, completed=1)

        containerconfig.copy_to_container(
            config.paths.local_backup,
            dst_dir = Path("/source/"),
            arcname = "pg_dump_out"
        )
        progress.update(copy_to_container, completed=1)

        time.sleep(5)
        containerconfig.restore()
        progress.update(container_restore, completed=1)


if __name__ == "__main__":
    app()
