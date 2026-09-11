/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.mapper.provider.base;

import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestSchemaMetaBaseSQLProvider {

  private static final SchemaMetaBaseSQLProvider PROVIDER = new SchemaMetaBaseSQLProvider();

  @Test
  void testSelectActiveChildChecksEverySupportedChildType() {
    String sql = PROVIDER.selectActiveChildBySchemaId(null);
    List<String> childTables =
        Arrays.asList(
            TableMetaMapper.TABLE_NAME,
            ViewMetaMapper.TABLE_NAME,
            FilesetMetaMapper.META_TABLE_NAME,
            FunctionMetaMapper.TABLE_NAME,
            ModelMetaMapper.TABLE_NAME,
            TopicMetaMapper.TABLE_NAME);

    childTables.forEach(
        tableName ->
            Assertions.assertTrue(
                sql.contains(
                    "FROM " + tableName + " WHERE schema_id = #{schemaId} AND deleted_at = 0"),
                () -> "Missing active-child check for " + tableName + " in: " + sql));
    Assertions.assertEquals(childTables.size() - 1, countOccurrences(sql, "UNION ALL"));
    Assertions.assertTrue(sql.endsWith("LIMIT 1"));
  }

  private static int countOccurrences(String value, String target) {
    return (value.length() - value.replace(target, "").length()) / target.length();
  }
}
