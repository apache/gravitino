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
package com.datastrato.gravitino.catalog.lakehouse.paimon.ops;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link PaimonCatalogOps}. */
public class TestPaimonCatalogOps {

  private PaimonCatalogOps paimonCatalogOps;
  @TempDir private File warehouse;

  private static final String DATABASE = "test_table_ops_database";
  private static final String TABLE = "test_table_ops_table";
  private static final String COMMENT = "table_ops_table_comment";
  private static final NameIdentifier IDENTIFIER = NameIdentifier.of(Namespace.of(DATABASE), TABLE);
  private static final Map<String, String> OPTIONS = ImmutableMap.of(BUCKET.key(), "10");

  @BeforeEach
  public void setUp() throws Exception {
    paimonCatalogOps =
        new PaimonCatalogOps(
            new PaimonConfig(
                ImmutableMap.of(PaimonCatalogPropertiesMetadata.WAREHOUSE, warehouse.getPath())));
    createDatabase();
  }

  @AfterEach
  public void tearDown() throws Exception {
    dropDatabase();
    if (paimonCatalogOps != null) {
      paimonCatalogOps.close();
    }
  }

  @Test
  void testTableOperations() throws Exception {
    // list tables
    Assertions.assertEquals(
        0, paimonCatalogOps.listTables(IDENTIFIER.namespace().toString()).size());

    // create table
    Pair<String, Schema> tableInfo =
        Pair.of(
            IDENTIFIER.toString(),
            Schema.newBuilder()
                .column("col_1", DataTypes.INT().notNull(), IntType.class.getSimpleName())
                .column("col_2", DataTypes.STRING(), VarCharType.class.getSimpleName())
                .column("col_3", DataTypes.STRING().notNull(), VarCharType.class.getSimpleName())
                .column(
                    "col_4",
                    DataTypes.ARRAY(
                        RowType.builder()
                            .field(
                                "sub_col_1",
                                DataTypes.DATE(),
                                RowType.class.getSimpleName() + DateType.class.getSimpleName())
                            .field(
                                "sub_col_2",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                RowType.class.getSimpleName() + MapType.class.getSimpleName())
                            .field(
                                "sub_col_3",
                                DataTypes.TIMESTAMP().notNull(),
                                RowType.class.getSimpleName() + TimestampType.class.getSimpleName())
                            .build()),
                    ArrayType.class.getSimpleName())
                .comment(COMMENT)
                .options(OPTIONS)
                .build());
    paimonCatalogOps.createTable(tableInfo.getKey(), tableInfo.getValue());

    // load table
    Table table = paimonCatalogOps.loadTable(IDENTIFIER.toString());

    assertEquals(TABLE, table.name());
    assertTrue(table.comment().isPresent());
    assertEquals(
        RowType.builder()
            .field("col_1", DataTypes.INT().notNull(), IntType.class.getSimpleName())
            .field("col_2", DataTypes.STRING(), VarCharType.class.getSimpleName())
            .field("col_3", DataTypes.STRING().notNull(), VarCharType.class.getSimpleName())
            .field(
                "col_4",
                DataTypes.ARRAY(
                    RowType.builder()
                        .field(
                            "sub_col_1",
                            DataTypes.DATE(),
                            RowType.class.getSimpleName() + DateType.class.getSimpleName())
                        .field(
                            "sub_col_2",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                            RowType.class.getSimpleName() + MapType.class.getSimpleName())
                        .field(
                            "sub_col_3",
                            DataTypes.TIMESTAMP().notNull(),
                            RowType.class.getSimpleName() + TimestampType.class.getSimpleName())
                        .build()),
                ArrayType.class.getSimpleName())
            .build()
            .toString(),
        table.rowType().toString());
    assertEquals(COMMENT, table.comment().get());
    assertEquals(OPTIONS.get(BUCKET.key()), table.options().get(BUCKET.key()));

    // TODO: alter table is unsupported now.

    // drop table
    Assertions.assertDoesNotThrow(() -> paimonCatalogOps.dropTable(IDENTIFIER.toString()));
    Assertions.assertThrowsExactly(
        Catalog.TableNotExistException.class,
        () -> paimonCatalogOps.dropTable(IDENTIFIER.toString()));

    // list table again
    Assertions.assertEquals(
        0, paimonCatalogOps.listTables(IDENTIFIER.namespace().toString()).size());

    // create a new table to make database not empty to test drop database cascade
    paimonCatalogOps.createTable(tableInfo.getKey(), tableInfo.getValue());
    Assertions.assertNotNull(paimonCatalogOps.loadTable(IDENTIFIER.toString()));
  }

  private void createDatabase() throws Exception {
    // list databases
    assertEquals(0, paimonCatalogOps.listDatabases().size());

    // create database
    paimonCatalogOps.createDatabase(DATABASE, Maps.newHashMap());
    assertEquals(1, paimonCatalogOps.listDatabases().size());
    // load database
    assertNotNull(paimonCatalogOps.loadDatabase(DATABASE));
  }

  private void dropDatabase() throws Exception {
    Assertions.assertEquals(1, paimonCatalogOps.listDatabases().size());
    Assertions.assertEquals(1, paimonCatalogOps.listTables(DATABASE).size());
    paimonCatalogOps.dropDatabase(DATABASE, true);
    Assertions.assertTrue(paimonCatalogOps.listDatabases().isEmpty());
  }
}
