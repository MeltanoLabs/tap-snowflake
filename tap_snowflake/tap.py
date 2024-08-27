"""Snowflake tap class."""

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_snowflake.client import SnowflakeStream


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
            required=False,
            secret=True,
            description="The password for your Snowflake user. Either one of [`password`, `private_key`, `private_key_path`] is required.",
        ),
        th.Property(
            "private_key",
            th.StringType,
            required=False,
            secret=True,
            description="The private key is used to connect to snowflake. Either one of [`password`, `private_key`, `private_key_path`] is required.",
        ),
        th.Property(
            "private_key_path",
            th.StringType,
            required=False,
            description="Path to where the private key is stored. The private key is used to connect to snowflake. Either one of [`password`, `private_key`, `private_key_path`] is required.",
        ),
        th.Property(
            "private_key_passphrase",
            th.StringType,
            required=False,
            secret=True,
            description="The passprhase used to protect the private key",
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
        th.Property(
            "tables",
            th.ArrayType(th.StringType),
            description=(
                "An array of the table names that you want to sync. The table names "
                "should be fully qualified, including schema and table name. "
                "NOTE: this limits discovery to the tables specified, for performance "
                "reasons. Do not specify `tables` if you intend to discover the entire "
                "available catalog."
            ),
        ),
    ).to_dict()
    default_stream_class = SnowflakeStream


if __name__ == "__main__":
    TapSnowflake.cli()
