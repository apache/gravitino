import logging

import httpx

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 配置连接信息
BASE_URL = "http://localhost:8090/"
METALAKE_NAME = "test"
CATALOG_NAME = "iceberg"
SCHEMA_NAME = "db"
TABLE_NAME = "test1"

session = httpx.Client(base_url=BASE_URL, timeout=30)


def get_table_structure(catalog_name, schema_name, table_name):
    """获取指定表的结构"""
    try:
        response = session.get(
            f"/api/metalakes/{METALAKE_NAME}/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}"
        )
        response.raise_for_status()
        logger.info(f"表结构响应: {response.text}")
        try:
            data = response.json()
            # 检查响应是否包含table字段
            if isinstance(data, dict) and "table" in data:
                return data["table"]
            return data
        except ValueError:
            logger.error(f"无法解析表结构响应为JSON: {response.text}")
            return {}
    except Exception as e:
        logger.error(f"获取表结构失败: {e}")
        return {}


def print_table_structure(table_structure):
    """打印表结构信息"""
    if not table_structure:
        logger.info("未找到表结构信息。")
        return

    logger.info(f"表名: {table_structure.get('name')}")
    logger.info(f"注释: {table_structure.get('comment')}")

    # 打印列信息
    columns = table_structure.get("columns", [])
    if columns:
        logger.info("列信息:")
        for column in columns:
            logger.info(
                f"  - {column.get('name')} ({column.get('type')}): {column.get('comment', '')}"
            )
    else:
        logger.info("未找到列信息。")

    # 打印分区信息
    partitioning = table_structure.get("partitioning", [])
    if partitioning:
        logger.info("分区信息:")
        for partition in partitioning:
            field_name = partition.get("fieldName", [""])[0]
            strategy = partition.get("strategy")
            logger.info(f"  - {field_name} (策略: {strategy})")
    else:
        logger.info("未找到分区信息。")

    # 打印分布信息
    distribution = table_structure.get("distribution")
    if distribution:
        strategy = distribution.get("strategy")
        logger.info(f"分布策略: {strategy}")
    else:
        logger.info("未找到分布信息。")

    # 打印排序信息
    sort_orders = table_structure.get("sortOrders", [])
    if sort_orders:
        logger.info("排序信息:")
        for sort_order in sort_orders:
            field_name = sort_order.get("sortTerm", {}).get("fieldName", [""])[0]
            direction = sort_order.get("direction")
            logger.info(f"  - {field_name} ({direction})")
    else:
        logger.info("未找到排序信息。")


if __name__ == "__main__":
    logger.info(f"开始查询表结构: {CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}")
    table_structure = get_table_structure(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
    print_table_structure(table_structure)
