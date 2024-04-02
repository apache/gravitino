"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from unittest.mock import patch
from . import fixtures


def services_fixtures(cls):
    @patch(
        "gravitino.service._Service.get_version", return_value=fixtures.services_version
    )
    @patch(
        "gravitino.service._Service.list_metalakes",
        return_value=fixtures.services_list_metalakes,
    )
    @patch(
        "gravitino.service._Service.get_metalake",
        return_value=fixtures.services_get_metalake,
    )
    @patch(
        "gravitino.service._Service.list_catalogs",
        return_value=fixtures.services_list_catalogs,
    )
    @patch(
        "gravitino.service._Service.get_catalog",
        return_value=fixtures.services_get_catalog,
    )
    @patch(
        "gravitino.service._Service.list_schemas",
        return_value=fixtures.services_list_schemas,
    )
    @patch(
        "gravitino.service._Service.get_schema",
        return_value=fixtures.services_get_schema,
    )
    @patch(
        "gravitino.service._Service.list_tables",
        return_value=fixtures.services_list_tables,
    )
    @patch(
        "gravitino.service._Service.get_table",
        return_value=fixtures.services_get_table,
    )
    class Wrapper(cls):
        pass

    return Wrapper
