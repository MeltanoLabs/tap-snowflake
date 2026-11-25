"""Unit tests for workload identity federation authentication."""

from __future__ import annotations

from unittest import mock

import pytest
import sqlalchemy as sa
from singer_sdk.exceptions import ConfigValidationError

from tap_snowflake.client import SnowflakeAuthMethod, SnowflakeConnector
from tap_snowflake.tap import TapSnowflake

WIF_CONFIG = {
    "user": "u",
    "account": "acct",
    "workload_identity_provider": "AWS",
}


def test_workload_identity_provider_selects_workload_identity():
    connector = SnowflakeConnector(config=WIF_CONFIG)

    assert connector.auth_method == SnowflakeAuthMethod.WORKLOAD_IDENTITY


@pytest.mark.parametrize("provider", ["AWS", "AZURE", "GCP"])
def test_workload_identity_connect_args(provider: str):
    config = {**WIF_CONFIG, "workload_identity_provider": provider}
    connector = SnowflakeConnector(config=config)

    with mock.patch("tap_snowflake.client.sqlalchemy.create_engine") as create_engine:
        connector.create_engine()

    connect_args = create_engine.call_args.kwargs["connect_args"]
    assert connect_args["authenticator"] == "WORKLOAD_IDENTITY"
    assert connect_args["workload_identity_provider"] == provider
    assert "private_key" not in connect_args


def test_workload_identity_url_has_no_password():
    connector = SnowflakeConnector(config=WIF_CONFIG)

    url = sa.engine.make_url(connector.get_sqlalchemy_url(WIF_CONFIG))

    assert not url.password
    assert "authenticator" not in url.query


def test_workload_identity_with_password_is_rejected():
    connector = SnowflakeConnector(config={**WIF_CONFIG, "password": "p"})

    with pytest.raises(ConfigValidationError, match="workload identity provider"):
        _ = connector.auth_method


def test_unknown_workload_identity_provider_is_rejected():
    with pytest.raises(ConfigValidationError):
        TapSnowflake(config={**WIF_CONFIG, "workload_identity_provider": "OIDC"})
