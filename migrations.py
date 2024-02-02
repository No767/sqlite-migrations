import datetime
import json
import os
import re
import sqlite3 as sqlite
import traceback
import uuid
from pathlib import Path
from typing import TypedDict, TypeVar

import click

BE = TypeVar("BE", bound=BaseException)

REVISION_FILE = re.compile(r"(?P<kind>V)(?P<version>[0-9]+)__(?P<description>.+).sql")
CONNECTION_URI = "db.sqlite"


class Revisions(TypedDict):
    # The version key represents the current activated version
    # So v1 means v1 is active and the next revision should be v2
    # In order for this to work the number has to be monotonically increasing
    # and have no gaps
    version: int
    database_uri: str


class Revision:
    __slots__ = ("kind", "version", "description", "file")

    def __init__(
        self, *, kind: str, version: int, description: str, file: Path
    ) -> None:
        self.kind: str = kind
        self.version: int = version
        self.description: str = description
        self.file: Path = file

    @classmethod
    def from_match(cls, match: re.Match[str], file: Path):
        return cls(
            kind=match.group("kind"),
            version=int(match.group("version")),
            description=match.group("description"),
            file=file,
        )


class Migrations:
    def __init__(
        self,
        *,
        filename: str = "migrations/revisions.json",
        migrations_path: str = "migrations",
    ):
        self.filename = filename
        self.migrations_path = migrations_path
        self.root: Path = Path(__file__).parent
        self.revisions: dict[int, Revision] = self.get_revisions()
        self.load()

    def load(self) -> None:
        self.ensure_path()
        data = self._load_metadata()
        self.version: int = data["version"]
        self.database_uri: str = data["database_uri"]

    def _load_metadata(self) -> Revisions:
        path = Path(self.filename)
        try:
            with open(path, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except FileNotFoundError:
            return {"version": 0, "database_uri": ""}

    def dump(self) -> Revisions:
        return {"version": self.version, "database_uri": self.database_uri}

    def save(self):
        temp = f"{self.filename}.{uuid.uuid4()}.tmp"
        with open(temp, "w", encoding="utf-8") as tmp:
            json.dump(self.dump(), tmp)

        # atomically move the file
        os.replace(temp, self.filename)

    def is_next_revision_taken(self) -> bool:
        return self.version + 1 in self.revisions

    @property
    def ordered_revisions(self) -> list[Revision]:
        return sorted(self.revisions.values(), key=lambda r: r.version)

    def create_revision(self, reason: str, *, kind: str = "V") -> Revision:
        cleaned = re.sub(r"\s", "_", reason)
        migrations_folder = Path(self.root) / self.migrations_path
        filename = f"{kind}{self.version + 1}__{cleaned}.sql"
        path = migrations_folder / filename

        stub = (
            f"-- Revises: V{self.version}\n"
            f"-- Creation Date: {datetime.datetime.utcnow()} UTC\n"
            f"-- Reason: {reason}\n\n"
        )

        with open(path, "w", encoding="utf-8", newline="\n") as fp:
            fp.write(stub)

        self.save()
        return Revision(
            kind=kind, description=reason, version=self.version + 1, file=path
        )

    def upgrade(self, connection: sqlite.Connection) -> int:
        ordered = self.ordered_revisions
        successes = 0

        with connection:
            for revision in ordered:
                if revision.version > self.version:
                    sql = revision.file.read_text("utf-8")
                    connection.execute(sql)
                    successes += 1

            self.version += successes
            self.save()
            connection.commit()
            return successes

    def display(self) -> None:
        ordered = self.ordered_revisions
        for revision in ordered:
            if revision.version > self.version:
                sql = revision.file.read_text("utf-8")
                click.echo(sql)

    def ensure_path(self) -> None:
        migrations_path = self.root / self.migrations_path
        migrations_path.mkdir(exist_ok=True)

    def get_revisions(self) -> dict[int, Revision]:
        result: dict[int, Revision] = {}
        for file in self.root.glob("migrations/*.sql"):
            match = REVISION_FILE.match(file.name)
            if match is not None:
                rev = Revision.from_match(match, file)
                result[rev.version] = rev

        return result


def run_upgrade(migrations: Migrations):
    connection: sqlite.Connection = sqlite.connect(migrations.database_uri)
    return migrations.upgrade(connection)


@click.group(short_help="database migrations util", options_metavar="[options]")
def main():
    pass


@main.command()
def init():
    """Initializes the database and runs all the current migrations"""
    migrations = Migrations()
    migrations.database_uri = CONNECTION_URI
    try:
        applied = run_upgrade(migrations)
        click.secho(
            f"Successfully initialized and applied {applied} revisions(s)",
            fg="green",
        )
    except Exception:
        traceback.print_exc()
        click.secho("failed to initialize and apply migrations due to error", fg="red")


@main.command()
@click.option("--reason", "-r", help="The reason for this revision.", required=True)
def migrate(reason: str):
    """Creates a new revision for you to edit"""
    migrations = Migrations()

    if migrations.is_next_revision_taken():
        click.echo(
            "an unapplied migration already exists for the next version, exiting"
        )
        click.secho(
            "hint: apply pending migrations with the `upgrade` command", bold=True
        )
        return
    revision = migrations.create_revision(reason)
    click.echo(f"Created revision V{revision.version!r}")


@main.command()
def current():
    """Shows the current version"""
    migrations = Migrations()
    click.echo(f"Version: {migrations.version}")


@main.command()
@click.option("--sql", help="Print the SQL instead of executing it", is_flag=True)
def upgrade(sql):
    """Upgrade to the latest version"""
    migrations = Migrations()
    if sql:
        migrations.display()
        return

    try:
        applied = run_upgrade(migrations)
        click.secho(f"Applied {applied} revision(s)", fg="green")
    except Exception:
        traceback.print_exc()
        click.secho("failed to apply migrations due to error", fg="red")


@main.command()
@click.option("--reverse", help="Print in reverse order (oldest first).", is_flag=True)
def log(reverse):
    """Displays the revision history"""
    migrations = Migrations()

    # Revisions is oldest first already
    revs = (
        reversed(migrations.ordered_revisions)
        if not reverse
        else migrations.ordered_revisions
    )
    for rev in revs:
        as_yellow = click.style(f"V{rev.version:>03}", fg="yellow")
        click.echo(f'{as_yellow} {rev.description.replace("_", " ")}')


if __name__ == "__main__":
    main()
