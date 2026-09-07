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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSemanticModelNormalizeDispatcher {

  private static final Namespace INPUT_NAMESPACE =
      Namespace.of("metalake", "catalog", "MixedSchema");
  private static final Namespace NORMALIZED_NAMESPACE =
      Namespace.of("metalake", "catalog", "MIXEDSCHEMA");
  private static final NameIdentifier INPUT_IDENT =
      NameIdentifier.of(INPUT_NAMESPACE, "SalesModel");
  private static final NameIdentifier NORMALIZED_IDENT =
      NameIdentifier.of(NORMALIZED_NAMESPACE, "SalesModel");

  private SemanticModelDispatcher delegate;
  private SemanticModelNormalizeDispatcher dispatcher;

  @BeforeEach
  public void setUp() throws Exception {
    delegate = mock(SemanticModelDispatcher.class);
    CatalogManager catalogManager = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.capability()).thenReturn(new ParentNormalizingCapability());
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    dispatcher = new SemanticModelNormalizeDispatcher(delegate, catalogManager);
  }

  @Test
  public void testParentUsesCatalogCapabilityButModelUsesStableRules() {
    SemanticModelDefinition definition = definition();
    SemanticModel model = mock(SemanticModel.class);
    when(delegate.createSemanticModel(NORMALIZED_IDENT, "comment", definition, Map.of()))
        .thenReturn(model);
    when(delegate.loadSemanticModel(NORMALIZED_IDENT)).thenReturn(model);
    when(delegate.semanticModelExists(NORMALIZED_IDENT)).thenReturn(true);
    when(delegate.listSemanticModels(NORMALIZED_NAMESPACE))
        .thenReturn(new NameIdentifier[] {NORMALIZED_IDENT});
    when(delegate.dropSemanticModel(NORMALIZED_IDENT)).thenReturn(true);

    assertEquals(
        model, dispatcher.createSemanticModel(INPUT_IDENT, "comment", definition, Map.of()));
    assertEquals(model, dispatcher.loadSemanticModel(INPUT_IDENT));
    assertTrue(dispatcher.semanticModelExists(INPUT_IDENT));
    assertEquals(NORMALIZED_IDENT, dispatcher.listSemanticModels(INPUT_NAMESPACE)[0]);
    assertTrue(dispatcher.dropSemanticModel(INPUT_IDENT));

    verify(delegate).createSemanticModel(NORMALIZED_IDENT, "comment", definition, Map.of());
    verify(delegate).loadSemanticModel(NORMALIZED_IDENT);
    verify(delegate).semanticModelExists(NORMALIZED_IDENT);
    verify(delegate).listSemanticModels(NORMALIZED_NAMESPACE);
    verify(delegate).dropSemanticModel(NORMALIZED_IDENT);
  }

  @Test
  public void testRenameUsesStableSemanticModelRules() {
    SemanticModel model = mock(SemanticModel.class);
    SemanticModelChange rename = SemanticModelChange.rename("RevenueModel");
    when(delegate.alterSemanticModel(NORMALIZED_IDENT, rename)).thenReturn(model);

    assertEquals(model, dispatcher.alterSemanticModel(INPUT_IDENT, rename));
    verify(delegate).alterSemanticModel(NORMALIZED_IDENT, rename);

    NameIdentifier invalidIdent = NameIdentifier.of(INPUT_NAMESPACE, "invalid model");
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.createSemanticModel(invalidIdent, null, definition(), Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dispatcher.alterSemanticModel(
                INPUT_IDENT, SemanticModelChange.rename("invalid model")));
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.alterSemanticModel(INPUT_IDENT, new SemanticModelChange[0]));
  }

  private static SemanticModelDefinition definition() {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .build();
    return SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build();
  }

  private static final class ParentNormalizingCapability implements Capability {

    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("Normalize names for this test");
    }

    @Override
    public String normalizeName(Scope scope, String name) {
      if (scope == Scope.SCHEMA) {
        return name.toUpperCase(Locale.ROOT);
      }
      if (scope == Scope.SEMANTIC_MODEL) {
        return name.toLowerCase(Locale.ROOT);
      }
      return name;
    }
  }
}
