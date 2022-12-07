"""Snowflake tap class."""


import collections
from typing import List, Mapping

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers._compat import metadata

from tap_snowflake.client import SnowflakeStream


# Introduced in python 3.10, added here for compatability
def packages_distributions() -> Mapping[str, List[str]]:
    pkg_to_dist = collections.defaultdict(list)
    for dist in metadata.distributions():
        for pkg in (dist.read_text("top_level.txt") or "").split():
            pkg_to_dist[pkg].append(dist.metadata["Name"])
    return dict(pkg_to_dist)


class TapSnowflake(SQLTap):
    """Snowflake tap class."""

    name = "tap-snowflake"
    package_name = "meltanolabs-tap-snowflake"

    # From https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters  # noqa: E501
    config_jsonschema = th.PropertiesList(
        th.Property(
            "user",
            th.StringType,
            required=True,
            description="The login name for your Snowflake user.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="The password for your Snowflake user.",
        ),
        th.Property(
            "account",
            th.StringType,
            required=True,
            description="Your account identifier. See [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).",  # noqa: E501
        ),
        th.Property(
            "database",
            th.StringType,
            description="The initial database for the Snowflake session.",
        ),
        th.Property(
            "schema",
            th.StringType,
            description="The initial schema for the Snowflake session.",
        ),
        th.Property(
            "warehouse",
            th.StringType,
            description="The initial warehouse for the session.",
        ),
        th.Property(
            "role",
            th.StringType,
            description="The initial role for the session.",
        ),
    ).to_dict()
    default_stream_class = SnowflakeStream

    # TODO: remove once PR merges; https://github.com/meltano/sdk/pull/1241
    @classproperty
    def plugin_version(cls) -> str:
        """Get version.

        Returns:
            The package version number.
        """
        # try to discover distribution version
        distribution_map = packages_distributions()
        distribution = distribution_map.get(cls.name.replace("-", "_"), [None])[0]
        if distribution:
            version = metadata.version(distribution)
        else:
            # try to discover module version
            try:
                version = metadata.version(cls.name)
            except metadata.PackageNotFoundError:
                version = "[could not be detected]"
        return version


if __name__ == "__main__":
    TapSnowflake.cli()
