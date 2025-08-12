from typing import Any, Dict

from gravitino.api.types.json_serdes.base import JsonSerializable
from gravitino.dto.rel.partitions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO


class PartitionDTOSerdes(JsonSerializable[PartitionDTO]):
    @classmethod
    def serialize(cls, data_type: PartitionDTO) -> Dict[str, Any]:
        return SerdesUtils.write_partition(data_type)
