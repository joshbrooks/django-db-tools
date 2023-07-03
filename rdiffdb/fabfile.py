#!/usr/bin/env python
from dataclasses import dataclass
from datetime import date
from functools import cached_property
import io
import os
from pathlib import Path
import random
import shutil
import string
import tarfile
from typing import Iterable, NamedTuple
from fabric import Connection
import docker
from docker.models.containers import Container


container_name = "partisipa_db"
image_name = "postgis/postgis:latest"

try:
    import rdiffbackup.run
except ImportError:
    raise ImportError("You may need to install librsync-dev for your OS")


class Paths(NamedTuple):
    backup: Path
    local_backup: Path
    destination: Path  # Local path to rsync a to
    temp: Path


@dataclass
class PgContainerSettings:
    image_name: str = "postgis/postgis:latest"
    container_name: str = "my-db"
    password: str = "post1234"
    pg_port: int = 49158
    database: str = "postgres"
    user: str = "postgres"
    host: str = "localhost"

    @property
    def connection_url(self):
        return f"postgis://{self.user}:{self.password}@{self.host}:{self.pg_port}/{self.database}"

    def get_container(self) -> Container:
        client = docker.from_env()
        try:
            return client.containers.get(self.container_name)
        except docker.errors.NotFound:
            pass

        container = client.containers.create(
            image=client.images.get(name=self.image_name),
            name=self.container_name,
            environment=dict(
                POSTGRES_DB=self.database,
                POSTGRES_USER=self.user,
                POSTGRES_PASSWORD=self.password,
            ),
            ports={"5432/tcp": self.pg_port},
        )
        return container

    def copy_to_container(
        self,
        source_dir: Path,
        dst_dir: Path = Path("/source/"),
        arcname: str = "pg_dump_out",
    ):
        """
        Copy a folder on the host to a container folder
        """
        container = self.get_container()
        container.start()
        # Container should be running

        # if container.status in {'running', 'created'}:
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w|") as tar:
            tar.add(source_dir, arcname=arcname)
        container.exec_run(f"mkdir -p {dst_dir}")
        print(f"Putting archive from {source_dir} to {dst_dir}")
        container.put_archive(dst_dir, stream.getvalue())
    
    def _restore_cmds(self):
        preamble = f"psql --user {self.user} -d postgres -c"
        preamble_db = f"psql --user {self.user} -d {self.database} -c"

        yield f"""{preamble} "DROP DATABASE {self.database}" """
        yield f"""{preamble} "CREATE DATABASE {self.database}" """
    
        if self.database == 'partisipa_db':
            # For Partisipa only: Create metabase group and iampartisipa
            yield from (
                f"""{preamble_db} "DROP ROLE IF EXISTS metabase_group" """,
                f"""{preamble_db} "DROP ROLE IF EXISTS metabase" """,
                f"""{preamble_db} "DROP ROLE IF EXISTS partisipa" """,
                f"""{preamble_db} "DROP ROLE IF EXISTS iampartisipa" """,
                f"""{preamble_db} "CREATE ROLE metabase_group" """,
                f"""{preamble_db} "CREATE ROLE metabase" """,
                f"""{preamble_db} "CREATE ROLE iampartisipa" """,
                f"""{preamble_db} "CREATE ROLE partisipa" """,
            )
        
        yield f"pg_restore --user {self.user} /source/pg_dump_out -d {self.database}"

    def restore(self):
        exitcode = 0
        container = self.get_container()
        for cmd in self._restore_cmds():
            exitcode, output = container.exec_run(cmd)
            if exitcode != 0:
                print(output)
                break


@dataclass
class Config:
    user: str
    host: str
    database: str

    def connection(self, user="root", forward_agent=True):
        return Connection(host=self.host, user=user, forward_agent=forward_agent)

    @cached_property
    def paths(self) -> Paths:
        today = date.today().isoformat()
        letters = string.ascii_lowercase
        rand = "".join(random.choice(letters) for i in range(10))
        temp = Path("/tmp") / Path("pg_dump_out") / f"{today}_{rand}"

        return Paths(
            # This is the path on the server to use as the backup.
            # Note that the date and a random string will be appended.
            backup=Path("pg_dump_out"),
            local_backup=Path("/tmp") / Path("pg_dump_out"),
            destination=Path.home() / "databases" / self.host / self.database,
            temp=temp,
        )

    def backup_db(self) -> Iterable[tuple[str, str]]:
        """
        Return a command to backup the postgres database
        to a given directory
        """
        opts = dict(format="directory", file=self.paths.temp, compress="0")
        args = " ".join(map(lambda a: f"--{a[0]}={a[1]}", opts.items()))
        yield from (
            [
                (
                    "Create temp directory",
                    f"mkdir -p {Path('/tmp') / self.paths.backup}",
                ),
                (
                    "Set user for temp directory",
                    f"chown {self.user}:{self.user} {Path('/tmp') / self.paths.backup}",
                ),
                (
                    "Run pg_dump on the server",
                    f"su - {self.user} -c 'pg_dump {args} {self.database}'",
                ),
            ]
        )

    def rdiff_backup(self) -> str:
        cmd = [
            "backup",
            f"root@{self.host}::{self.paths.temp}",
            f"{self.paths.destination}",
        ]
        os.makedirs(self.paths.destination, exist_ok=True)
        return rdiffbackup.run.main_run(cmd)

    def run_backup_db(self):
        c = self.connection()
        for command_description, command in self.backup_db():
            yield command_description
            result = c.run(command)
            yield bool(result)

    def restore_as_of_now(self):
        """
        Run rdiff-backup with the `restore-as-of-now` option
        This will restore from `destination` to `local backup`
        """
        # Ensure backup directory parent is created
        backup = self.paths.local_backup
        shutil.rmtree(backup, ignore_errors=True)
        os.makedirs(backup.parent, exist_ok=True)
        cmd = ["--force", "--restore-as-of=now", self.paths.destination, backup]
        rdiffbackup.run.main_run(list(map(str, cmd)))
        return backup

    def rdiff_command(self, command: str) -> str:
        rdiffbackup.run.main_run(
            "list",
            "increments",
        )

    def list_backups(self):
        rdiffbackup.run.main_run(['list', 'increments', str(self.paths.destination)])


if __name__ == "__main__":
    from settings import hosts, containersettings
    config = hosts.get("partisipa")
    containerconfig = containersettings.get("partisipa")
    # c.run_backup_db()
    # c.rdiff_backup()
    # config.restore_as_of_now()

    # container = containerconfig.get_container()
    # containerconfig.copy_to_container(
    #     config.paths.local_backup,
    #     dst_dir = Path("/source/"),
    #     arcname = "pg_dump_out"
    # )
    # containerconfig.restore()
    # print(containerconfig.connection_url)
    print(config.list_backups())

    # make_backup(config, c)
    # restore_as_of_now(config)
    # backup_dir = restore_as_of_now()
    # print(f"Backup created at {backup_dir}")

    # container = get_or_start_container(container_name, image_name)

    # if container.status in {'running', 'created'}:

    #     copy_to_container(container, backup_dir)
    #     # shutil.rmtree(backup_dir)
