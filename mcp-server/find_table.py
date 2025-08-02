# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging

import httpx

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 服务器配置
BASE_URL = "http://localhost:8090/"
METALAKE_NAME = "test"

# 创建 HTTP 客户端
session = httpx.Client(base_url=BASE_URL)


def get_catalogs():
    """获取所有目录"""
    try:
        response = session.get(f"/api/metalakes/{METALAKE_NAME}/catalogs")
        response.raise_for_status()
        logger.info(f"目录响应: {response.text}")
        try:
            data = response.json()
            logger.info(f"解析后的目录数据: {data}")
            # 检查响应是否包含identifiers字段
            if isinstance(data, dict) and "identifiers" in data:
                return data["identifiers"]
            return data
        except ValueError:
            logger.error(f"无法解析目录响应为JSON: {response.text}")
            return response.text.strip().split("\n")
    except Exception as e:
        logger.error(f"获取目录失败: {e}")
        return []


def get_schemas(catalog_name):
    """获取指定目录下的所有模式"""
    try:
        response = session.get(
            f"/api/metalakes/{METALAKE_NAME}/catalogs/{catalog_name}/schemas"
        )
        response.raise_for_status()
        logger.info(f"模式响应 (目录: {catalog_name}): {response.text}")
        try:
            data = response.json()
            logger.info(f"解析后的模式数据 (目录: {catalog_name}): {data}")
            # 检查响应是否包含identifiers字段
            if isinstance(data, dict) and "identifiers" in data:
                return data["identifiers"]
            return data
        except ValueError:
            logger.error(
                f"无法解析模式响应为JSON (目录: {catalog_name}): {response.text}"
            )
            return response.text.strip().split("\n")
    except Exception as e:
        logger.error(f"获取模式失败 (目录: {catalog_name}): {e}")
        return []


def get_tables(catalog_name, schema_name):
    """获取指定目录和模式下的所有表"""
    try:
        response = session.get(
            f"/api/metalakes/{METALAKE_NAME}/catalogs/{catalog_name}/schemas/{schema_name}/tables"
        )
        response.raise_for_status()
        logger.info(
            f"表响应 (目录: {catalog_name}, 模式: {schema_name}): {response.text}"
        )
        try:
            data = response.json()
            logger.info(
                f"解析后的表数据 (目录: {catalog_name}, 模式: {schema_name}): {data}"
            )
            # 检查响应是否包含identifiers字段
            if isinstance(data, dict) and "identifiers" in data:
                return data["identifiers"]
            return data
        except ValueError:
            logger.error(
                f"无法解析表响应为JSON (目录: {catalog_name}, 模式: {schema_name}): {response.text}"
            )
            return response.text.strip().split("\n")
    except Exception as e:
        logger.error(f"获取表失败 (目录: {catalog_name}, 模式: {schema_name}): {e}")
        return []


def find_table(table_name):
    """查找表所在的目录和模式"""
    logger.info(f"开始查找表: {table_name}")

    # 获取所有目录
    catalogs = get_catalogs()
    logger.info(f"找到 {len(catalogs)} 个目录")

    # 遍历每个目录
    for catalog in catalogs:
        # 处理不同格式的目录数据
        if isinstance(catalog, dict):
            catalog_name = catalog.get("name")
        elif isinstance(catalog, str):
            catalog_name = catalog.strip()
        else:
            catalog_name = str(catalog)

        logger.info(f"正在检查目录: {catalog_name}")

        # 获取当前目录下的所有模式
        schemas = get_schemas(catalog_name)
        logger.info(f"  找到 {len(schemas)} 个模式")

        # 遍历每个模式
        for schema in schemas:
            # 处理不同格式的模式数据
            if isinstance(schema, dict):
                schema_name = schema.get("name")
            elif isinstance(schema, str):
                schema_name = schema.strip()
            else:
                schema_name = str(schema)

            logger.info(f"  正在检查模式: {schema_name}")

            # 获取当前模式下的所有表
            tables = get_tables(catalog_name, schema_name)
            logger.info(f"    找到 {len(tables)} 个表")

            # 检查是否包含目标表
            for table in tables:
                # 处理不同格式的表数据
                if isinstance(table, dict):
                    table_name_in_response = table.get("name")
                elif isinstance(table, str):
                    table_name_in_response = table.strip()
                else:
                    table_name_in_response = str(table)

                if table_name_in_response == table_name:
                    logger.info(f"找到表 {table_name}!")
                    logger.info(f"目录: {catalog_name}")
                    logger.info(f"模式: {schema_name}")
                    return {"catalog": catalog_name, "schema": schema_name}

    logger.info(f"未找到表 {table_name}")
    return None


if __name__ == "__main__":
    result = find_table("test1")
    if result:
        print(
            f"表 'test1' 位于目录 '{result['catalog']}' 和模式 '{result['schema']}' 下。"
        )
    else:
        print("未找到表 'test1'。")
