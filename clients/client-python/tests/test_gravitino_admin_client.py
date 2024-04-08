"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import gravitino
from gravitino.client.gravitino_admin_client import GravitinoAdminClient
from gravitino.dto.dto_converters import DTOConverters
from gravitino.dto.requests.metalake_updates_request import MetalakeUpdatesRequest
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.meta_change import MetalakeChange
from gravitino.name_identifier import NameIdentifier
from gravitino.utils.exceptions import NotFoundError
from tests.integration_test_env import IntegrationTestEnv


class TestGravitinoClient(IntegrationTestEnv):
    def setUp(self):
        self._gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")

    def test_create_metalake(self):
        metalake_name = "metalake00"
        try:
            self.create_metalake(metalake_name)
        except gravitino.utils.exceptions.HTTPError:
            self.drop_metalake(metalake_name)

        # Clean test data
        self.drop_metalake(metalake_name)

    def create_metalake(self, metalake_name):
        comment = "This is a sample comment"
        ident = NameIdentifier.of(metalake_name)
        properties = {"key1": "value1", "key2": "value2"}

        gravitinoMetalake = self._gravitino_admin_client.create_metalake(ident, comment, properties)

        self.assertEqual(gravitinoMetalake.name, metalake_name)
        self.assertEqual(gravitinoMetalake.comment, comment)
        self.assertEqual(gravitinoMetalake.properties.get("key1"), "value1")
        self.assertEqual(gravitinoMetalake.audit.creator, "anonymous")

    def test_alter_metalake(self):
        metalake_name = "metalake02"
        metalake_new_name = metalake_name + "_new"
        try:
            self.create_metalake(metalake_name)
        except gravitino.utils.exceptions.HTTPError:
            self.drop_metalake(metalake_name)

        changes = (
            MetalakeChange.rename(metalake_new_name),
            MetalakeChange.update_comment("new metalake comment"),
        )

        metalake = self._gravitino_admin_client.alter_metalake(NameIdentifier.of(metalake_name), *changes)
        self.assertEqual(metalake_new_name, metalake.name)
        self.assertEqual("new metalake comment", metalake.comment)
        self.assertEqual("anonymous", metalake.audit.creator)  # Assuming a constant or similar attribute

        # Reload metadata via new name to check if the changes are applied
        new_metalake = self._gravitino_admin_client.load_metalake(NameIdentifier.of(metalake_new_name))
        self.assertEqual(metalake_new_name, new_metalake.name)
        self.assertEqual("new metalake comment", new_metalake.comment)

        # Old name does not exist
        old = NameIdentifier.of(metalake_name)
        with self.assertRaises(NotFoundError):  # TODO: NoSuchMetalakeException
            self._gravitino_admin_client.load_metalake(old)

        # Clean test data
        self.drop_metalake(metalake_name)
        self.drop_metalake(metalake_new_name)

    def drop_metalake(self, metalake_name):
        ident = NameIdentifier.of(metalake_name)
        self.assertTrue(self._gravitino_admin_client.drop_metalake(ident))

    def test_drop_metalake(self):
        metalake_name = "metalake03"
        try:
            self.create_metalake(metalake_name)
        except gravitino.utils.exceptions.HTTPError:
            self.drop_metalake(metalake_name)

        self.drop_metalake(metalake_name)

    def test_metalake_update_request_to_json(self):
        changes = (
            MetalakeChange.rename("my_metalake_new"),
            MetalakeChange.update_comment("new metalake comment"),
        )
        reqs = [DTOConverters.to_metalake_update_request(change) for change in changes]
        updates_request = MetalakeUpdatesRequest(reqs)
        valid_json = (f'{{"updates": [{{"@type": "rename", "newName": "my_metalake_new"}}, '
                      f'{{"@type": "updateComment", "newComment": "new metalake comment"}}]}}')
        self.assertEqual(updates_request.to_json(), valid_json)

    def test_from_json_metalake_response(self):
        str = (b'{"code":0,"metalake":{"name":"example_name18","comment":"This is a sample comment",'
               b'"properties":{"key1":"value1","key2":"value2"},'
               b'"audit":{"creator":"anonymous","createTime":"2024-04-05T10:10:35.218Z"}}}')
        metalake_response = MetalakeResponse.from_json(str, infer_missing=True)
        self.assertEqual(metalake_response.code, 0)
        self.assertIsNotNone(metalake_response.metalake)
        self.assertEqual(metalake_response.metalake.name, "example_name18")
        self.assertEqual(metalake_response.metalake.audit.creator, "anonymous")

    def test_list_metalakes(self):
        metalake_name = "metalake05"
        self.create_metalake(metalake_name)
        object = self._gravitino_admin_client.list_metalakes()
        assert len(object) > 0

        # Clean test data
        self.drop_metalake(metalake_name)

