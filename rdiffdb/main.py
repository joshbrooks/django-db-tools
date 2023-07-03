#!/usr/bin/env python

import time
import typer
from settings import hosts
from rich.progress import Progress, SpinnerColumn, TextColumn

app = typer.Typer()


@app.command()
def list_hosts():
    print(hosts.keys())


@app.command()
def backup_db(host: str):
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
        task = progress.add_task(description="Rdiff-backup to your local", total=None)
        h.rdiff_backup()
        progress.update(task_id=task, completed=1)


if __name__ == "__main__":
    app()
