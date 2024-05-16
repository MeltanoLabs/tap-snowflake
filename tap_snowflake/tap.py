"""Snowflake tap class."""

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_snowflake.client import SnowflakeStream


class TapSnowflake(SQLTap):
    """Snowflake tap class."""
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
            description="The password for your Snowflake user. Either this or the private key is required.",
        ),
        th.Property(
            "private_key",
            th.StringType,
            description="The private key to authenticate your Snowflake user. Either this or the password is required.",
        ),
        th.Property(
            "private_key_passphrase",
            th.StringType,
            description="The passphrase for your encrypted private key to authenticate your Snowflake user.",
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

    name = "tap-snowflake"
    package_name = "meltanolabs-tap-snowflake"

    # From https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters  # noqa: E501
    default_stream_class = SnowflakeStream


if __name__ == "__main__":
    TapSnowflake.cli()
