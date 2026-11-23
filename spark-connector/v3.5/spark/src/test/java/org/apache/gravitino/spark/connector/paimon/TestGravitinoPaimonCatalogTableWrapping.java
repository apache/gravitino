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

package org.apache.gravitino.spark.connector.paimon;

import static org.mockito.Mockito.mock;

import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.paimon.spark.SparkTable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests how {@link GravitinoPaimonCatalog} wraps the table Paimon's SparkCatalog loads. Paimon
 * returns a {@link SparkTable} for its file store tables and another Spark table for format tables
 * (a FileTable, or a PaimonFormatTable in Paimon 1.4.1 and later), so the wrapping must not assume
 * a {@link SparkTable}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGravitinoPaimonCatalogTableWrapping {

  private static final Identifier IDENT = Identifier.of(new String[] {"db"}, "t");

  private GravitinoPaimonCatalog catalog;

  @BeforeAll
  void setUp() {
    GravitinoCatalogManager.create(
        new SparkConf(false), "user", identity -> mock(GravitinoClient.class));
    catalog = new GravitinoPaimonCatalogSpark35();
  }

  @AfterAll
  void tearDown() {
    GravitinoCatalogManager.get().close();
  }

  @Test
  void testNonPaimonFileStoreTableIsReturnedAsIs() {
    // Stands in for the table Paimon returns for a format table: any Spark table that is not a
    // Paimon SparkTable.
    Table formatTable = mock(Table.class);

    Table result = wrap(formatTable);

    Assertions.assertSame(formatTable, result);
  }

  @Test
  void testPaimonFileStoreTableIsWrapped() {
    SparkTable paimonTable = mock(SparkTable.class);

    Table result = wrap(paimonTable);

    Assertions.assertInstanceOf(SparkPaimonTable.class, result);
  }

  private Table wrap(Table sparkTable) {
    return catalog.createSparkTable(
        IDENT,
        mock(org.apache.gravitino.rel.Table.class),
        sparkTable,
        mock(TableCatalog.class),
        PaimonPropertiesConverter.getInstance(),
        new SparkTransformConverter(true),
        new SparkTypeConverter());
  }
}
