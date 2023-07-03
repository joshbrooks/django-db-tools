# Settings for various hosts information
from rdiffdb.fabfile import Config, PgContainerSettings


hosts: dict[str, Config] = dict(
    dird=Config(
        user="dird",
        host="dird-staging.catalpa.build",
        database="dird_db",
    ),
    partisipa=Config(
        user="partisipa",
        host="partisipa-staging.catalpa.build",
        database="partisipa_db",
    ),
    partisipa_metabase=Config(
        user="partisipa",
        host="partisipa-staging.catalpa.build",
        database="partisipa_metabase_db",
    ),
)

containersettings: dict[str, PgContainerSettings] = dict(
    partisipa=PgContainerSettings(
        container_name="partisipa_db", user="partisipa", database="partisipa_db"
    ),
    partisipa_metabase=PgContainerSettings(
        container_name="partisipa_metabase_db",
        user="partisipa_metabase",
        database="partisipa_metabase_db",
        pg_port=49159,
    ),
)
