import unittest

from gravitino.api.types.types import Types
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO
from gravitino.dto.rel.partitions.identity_partition_dto import IdentityPartitionDTO
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO


class TestPartitionDTOs(unittest.TestCase):
    def test_identity_partition_dto(self):
        partition_name = "dt=2025-08-08/country=us"
        field_names = [["dt"], ["country"]]
        properties = {}
        values = [
            LiteralDTO.builder()
            .with_data_type(data_type=Types.DateType.get())
            .with_value(value="2025-08-08")
            .build(),
            LiteralDTO.builder()
            .with_data_type(data_type=Types.StringType.get())
            .with_value(value="us")
            .build(),
        ]
        dto = IdentityPartitionDTO(
            name=partition_name,
            field_names=field_names,
            values=values,
            properties=properties,
        )

        similar_dto = IdentityPartitionDTO(
            name=partition_name,
            field_names=field_names,
            values=values,
            properties=properties,
        )

        different_dto = IdentityPartitionDTO(
            name="different_partition",
            field_names=field_names,
            values=values,
            properties={},
        )

        dtos = {dto: 1, similar_dto: 2, different_dto: 3}

        self.assertIsInstance(dto, IdentityPartitionDTO)
        self.assertIs(dto.type(), PartitionDTO.Type.IDENTITY)
        self.assertEqual(dto.name(), partition_name)
        self.assertListEqual(dto.field_names(), field_names)
        self.assertListEqual(dto.values(), values)
        self.assertDictEqual(dto.properties(), properties)

        self.assertTrue(dto == dto)
        self.assertTrue(dto == similar_dto)
        self.assertFalse(dto == different_dto)

        self.assertEqual(len(dtos), 2)
        self.assertEqual(dtos[dto], 2)
