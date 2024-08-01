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

package org.apache.gravitino.catalog.mysql.integration.test;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class CatalogMysqlVersion5IT extends CatalogMysqlIT {
  public CatalogMysqlVersion5IT() {
    super();
    mysqlImageName = "mysql:5.7";
  }

  boolean SupportColumnDefaultValueExpression() {
    return false;
  }

  @Test
  void testColumnDefaultValue() {
    Column col1 =
        Column.of(
            MYSQL_COL_NAME2,
            Types.TimestampType.withoutTimeZone(),
            "col_2_comment",
            false,
            false,
            FunctionExpression.of("current_timestamp"));
    Column col2 =
        Column.of(
            MYSQL_COL_NAME3,
            Types.VarCharType.of(255),
            "col_3_comment",
            true,
            false,
            Literals.NULL);
    Column col3 =
        Column.of(MYSQL_COL_NAME4, Types.StringType.get(), "col_4_comment", false, false, null);
    Column col4 =
        Column.of(
            MYSQL_COL_NAME5,
            Types.VarCharType.of(255),
            "col_5_comment",
            true,
            false,
            Literals.stringLiteral("current_timestamp"));

    Column[] newColumns = new Column[] {col1, col2, col3, col4};

    Table createdTable =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of(schemaName, GravitinoITUtils.genRandomName("mysql_it_table")),
                newColumns,
                null,
                ImmutableMap.of());

    Assertions.assertEquals(
        DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, createdTable.columns()[0].defaultValue());
    Assertions.assertEquals(Literals.NULL, createdTable.columns()[1].defaultValue());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, createdTable.columns()[2].defaultValue());
    Assertions.assertEquals(
        Literals.varcharLiteral(255, "current_timestamp"),
        createdTable.columns()[3].defaultValue());
  }
}
