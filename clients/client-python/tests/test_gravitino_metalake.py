# from datetime import datetime
# from unittest import TestCase
#
# from gravitino.client.gravitino_client import GravitinoClient
# from gravitino.dto.audit_dto import AuditDTO
# from gravitino.dto.metalake_dto import MetalakeDTO
# from gravitino.dto.requests.metalake_create_request import MetalakeCreateRequest
#
#
# class TestGravitinoMetalake(TestCase):
#     metalake_name = "test"
#     provider = "test"
#     gravitino_client = None
#
#     @classmethod
#     def setUpClass(cls):
#         cls.setUp()
#         cls.create_metalake(cls.client, cls.metalake_name)
#
#         mock_metalake = {
#             "name": cls.metalake_name,
#             "comment": "comment",
#             "audit": {
#                 "creator": "creator",
#                 "create_time": datetime.now().isoformat()
#             }
#         }
#         resp = {
#             "metalake": mock_metalake
#         }
#
#         with Mocker() as m:
#             m.get(f"/api/metalakes/{cls.metalake_name}", json=resp, status_code=200)
#
#         cls.gravitino_client = GravitinoClient(f"http://127.0.0.18090",
#                                                metalake_name=cls.metalake_name)
#
#     @staticmethod
#     def create_metalake(client, metalake_name):
#         mock_metalake = MetalakeDTO(name=metalake_name,comment="comment",audit=AuditDTO(creator="creator", create_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#
#         req = MetalakeCreateRequest(name=TestGravitinoMetalake.metalake_name, comment="comment", properties={})
#         resp = MetalakeResponse(mock_metalake)
#         build_mock_resource(Method.POST, "/api/metalakes", req, resp, HttpStatus.SC_OK)
#
#         return client.create_metalake(NameIdentifier.parse(metalake_name), "comment", {})
#
#     请注意，这只是一
#
#
# # 示例用法
# TestGravitinoMetalake.setUpClass()
