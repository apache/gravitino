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

package org.apache.gravitino.flink.connector.jdbc;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.gravitino.flink.connector.jdbc.mysql.MysqlPropertiesConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMysqlPropertiesConverter extends AbstractJdbcPropertiesConverterTestSuite {

  @Override
  protected JdbcPropertiesConverter getConverter(Map<String, String> catalogOptions) {
    return MysqlPropertiesConverter.INSTANCE;
  }

  @Test
  public void testToFlinkTableProperties() {
    String schema = "myDatabase";
    String tableName = "myTable";
    Map<String, String> flinkCatalogProperties =
        getConverter(catalogProperties).toFlinkCatalogProperties(catalogProperties);
    Map<String, String> tableProperties =
        getConverter(catalogProperties)
            .toFlinkTableProperties(
                flinkCatalogProperties, ImmutableMap.of(), new ObjectPath(schema, tableName));

    // For MySQL, the Flink "database" (schema) is itself the connection database.
    Assertions.assertEquals(
        flinkUrl + schema,
        tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_DATABASE_URL));
    Assertions.assertEquals(
        tableName, tableProperties.get(JdbcPropertiesConstants.FLINK_JDBC_TABLE_NAME));
  }
}
