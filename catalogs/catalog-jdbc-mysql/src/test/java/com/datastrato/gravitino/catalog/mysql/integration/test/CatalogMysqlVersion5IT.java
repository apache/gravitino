/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.mysql.integration.test;

import static com.datastrato.gravitino.dto.util.DTOConverters.toFunctionArg;
import static com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
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
                NameIdentifier.of(
                    metalakeName,
                    catalogName,
                    schemaName,
                    GravitinoITUtils.genRandomName("mysql_it_table")),
                newColumns,
                null,
                ImmutableMap.of());

    Assertions.assertEquals(
        toFunctionArg(DEFAULT_VALUE_OF_CURRENT_TIMESTAMP),
        createdTable.columns()[0].defaultValue());
    Assertions.assertEquals(toFunctionArg(Literals.NULL), createdTable.columns()[1].defaultValue());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, createdTable.columns()[2].defaultValue());
    Assertions.assertEquals(
        new LiteralDTO.Builder()
            .withValue("current_timestamp")
            .withDataType(Types.VarCharType.of(255))
            .build(),
        createdTable.columns()[3].defaultValue());
  }
}
