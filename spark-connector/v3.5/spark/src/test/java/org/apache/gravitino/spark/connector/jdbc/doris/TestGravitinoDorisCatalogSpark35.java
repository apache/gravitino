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

package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Unit tests for the governed Doris catalog boundary. */
public class TestGravitinoDorisCatalogSpark35 {

  @Test
  void testMissingVendedJdbcCredentialFailsClosed() {
    Catalog catalog = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(catalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[0]);

    assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoDorisCatalogSpark35.requiredJdbcCredential(catalog));
  }

  @Test
  @SuppressWarnings("deprecation")
  void testWritesAndCatalogDdlAreRejectedBeforeAccessingTheBackend() {
    GravitinoClient client = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    try {
      GravitinoDorisCatalogSpark35 catalog = new GravitinoDorisCatalogSpark35();
      Identifier identifier = Identifier.of(new String[] {"db"}, "table");
      List<Executable> operations =
          List.of(
              () -> catalog.loadTable(identifier, Set.of()),
              () -> catalog.createTable(identifier, new StructType(), new Transform[0], Map.of()),
              () -> catalog.alterTable(identifier),
              () -> catalog.dropTable(identifier),
              () -> catalog.purgeTable(identifier),
              () -> catalog.renameTable(identifier, Identifier.of(new String[] {"db"}, "renamed")),
              () -> catalog.createNamespace(new String[] {"db"}, Map.of()),
              () -> catalog.alterNamespace(new String[] {"db"}),
              () -> catalog.dropNamespace(new String[] {"db"}, true));
      for (Executable operation : operations) {
        assertThrows(UnsupportedOperationException.class, operation);
      }
      verifyNoInteractions(client);
    } finally {
      GravitinoCatalogManager.get().close();
    }
  }
}
