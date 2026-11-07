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
package org.apache.gravitino.spark.connector.glue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange.AddColumn;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGravitinoGlueCatalogSpark34 {

  private static final Map<String, String> ICEBERG_TABLE_PROPERTIES =
      ImmutableMap.of("table-format", "ICEBERG");

  @BeforeAll
  void initCatalogManager() {
    GravitinoClient mockClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create("test_metalake", () -> mockClient);
  }

  @AfterAll
  void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @Test
  void testIcebergTablesUseIcebergTimestampTypes() {
    SparkTypeConverter converter =
        new GravitinoGlueCatalogSpark34().getSparkTypeConverter(ICEBERG_TABLE_PROPERTIES);

    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(), converter.toGravitinoType(DataTypes.TimestampType));
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(),
        converter.toGravitinoType(DataTypes.TimestampNTZType));
    Assertions.assertEquals(
        DataTypes.TimestampNTZType, converter.toSparkType(Types.TimestampType.withoutTimeZone(6)));
  }

  @Test
  void testIcebergTableChangesAcceptTimestampNTZ() {
    Table icebergTable = mock(Table.class);
    when(icebergTable.properties()).thenReturn(ICEBERG_TABLE_PROPERTIES);
    GravitinoGlueCatalogSpark34 catalog =
        new GravitinoGlueCatalogSpark34() {
          @Override
          protected Table loadGravitinoTable(Identifier ident) {
            return icebergTable;
          }
        };
    TableChange addColumn = TableChange.addColumn(new String[] {"ts"}, DataTypes.TimestampNTZType);

    org.apache.gravitino.rel.TableChange change =
        catalog
            .getSparkTableChangeConverter(
                Identifier.of(new String[] {"db"}, "tbl"), new TableChange[] {addColumn})
            .toGravitinoTableChange(addColumn);

    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(), ((AddColumn) change).getDataType());
  }
}
