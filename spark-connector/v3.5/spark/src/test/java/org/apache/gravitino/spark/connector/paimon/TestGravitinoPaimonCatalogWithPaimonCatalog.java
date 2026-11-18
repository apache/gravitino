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
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests that {@link GravitinoPaimonCatalog} exposes the Paimon catalog it wraps. Paimon procedures
 * such as {@code remove_orphan_files} cast the catalog they run in to {@link WithPaimonCatalog}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGravitinoPaimonCatalogWithPaimonCatalog {

  @BeforeAll
  void setUp() {
    GravitinoCatalogManager.create(
        new SparkConf(false), "user", identity -> mock(GravitinoClient.class));
  }

  @AfterAll
  void tearDown() {
    GravitinoCatalogManager.get().close();
  }

  @Test
  void testDelegatesToTheUnderlyingPaimonCatalog() throws Exception {
    Catalog paimonCatalog = mock(Catalog.class);
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.paimonCatalog()).thenReturn(paimonCatalog);
    when(sparkCatalog.name()).thenReturn("paimon_backend");

    GravitinoPaimonCatalog catalog = new GravitinoPaimonCatalogSpark35();
    Field field = BaseCatalog.class.getDeclaredField("sparkCatalog");
    field.setAccessible(true);
    field.set(catalog, sparkCatalog);

    Assertions.assertInstanceOf(WithPaimonCatalog.class, catalog);
    Assertions.assertSame(paimonCatalog, ((WithPaimonCatalog) catalog).paimonCatalog());
    Assertions.assertEquals("paimon_backend", catalog.paimonCatalogName());
  }
}
