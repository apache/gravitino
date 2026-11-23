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
package org.apache.gravitino.catalog;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class TestSchemaDropExistingNames {
  @ParameterizedTest
  @ValueSource(ints = {64, 65, 100})
  void testExistingNamesUseExistingCapabilityAndKeepCaseFolding(int length) {
    CatalogManager manager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(manager, catalog);
    Capability capability =
        new Capability() {
          /** {@inheritDoc} */
          @Override
          public CapabilityResult specificationOnExistingName(Scope scope, String name) {
            return CapabilityResult.SUPPORTED;
          }
          /** {@inheritDoc} */
          @Override
          public CapabilityResult caseSensitiveOnName(Scope scope) {
            return CapabilityResult.unsupported("case insensitive");
          }
        };
    Mockito.when(catalog.capability()).thenReturn(capability);
    SchemaDispatcher delegate = Mockito.mock(SchemaDispatcher.class);
    SchemaNormalizeDispatcher dispatcher = new SchemaNormalizeDispatcher(delegate, manager);
    String name = "N".repeat(length);
    NameIdentifier requested = NameIdentifier.of("ml", "cat", name);
    dispatcher.dropSchema(requested, true);
    Mockito.verify(delegate).dropSchema(NameIdentifier.of("ml", "cat", name.toLowerCase()), true);
    if (length > 64) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> dispatcher.createSchema(requested, null, null));
      Mockito.verify(delegate, Mockito.never())
          .createSchema(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"bad?name", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789long"})
  void testDefaultExistingNameRestrictionsArePreserved(String name) {
    CatalogManager manager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(manager, catalog);
    Mockito.when(catalog.capability()).thenReturn(new Capability() {});
    SchemaDispatcher delegate = Mockito.mock(SchemaDispatcher.class);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new SchemaNormalizeDispatcher(delegate, manager)
                .dropSchema(NameIdentifier.of("ml", "cat", name), false));
    Mockito.verifyNoInteractions(delegate);
  }
}
