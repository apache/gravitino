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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

/** Tests the Core Semantic Model dispatcher contract. */
public class TestSemanticModelDispatcher {

  @Test
  void testDispatcherDoesNotExposeTwoArgumentImport() {
    assertFalse(SemanticModelCatalog.class.isAssignableFrom(SemanticModelDispatcher.class));
    assertThrows(
        NoSuchMethodException.class,
        () ->
            SemanticModelDispatcher.class.getMethod(
                "importOssieDocument", Namespace.class, OssieDocument.class));
  }

  @Test
  void testParsedImportRequiresAnExplicitImplementation() throws NoSuchMethodException {
    assertFalse(
        SemanticModelDispatcher.class
            .getMethod(
                "importSemanticModel",
                NameIdentifier.class,
                String.class,
                SemanticModelDefinition.class,
                Map.class)
            .isDefault());
  }

  @Test
  void testExportRequiresAnExplicitImplementation() throws NoSuchMethodException {
    assertFalse(
        SemanticModelDispatcher.class
            .getMethod("exportOssieDocument", NameIdentifier.class, OssieFormat.class)
            .isDefault());
  }

  @Test
  void testSemanticModelExists() {
    SemanticModelDispatcher dispatcher = mock(SemanticModelDispatcher.class, CALLS_REAL_METHODS);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "model");
    when(dispatcher.loadSemanticModel(ident)).thenReturn(mock(SemanticModel.class));
    assertTrue(dispatcher.semanticModelExists(ident));

    when(dispatcher.loadSemanticModel(ident))
        .thenThrow(new NoSuchSemanticModelException("Semantic Model does not exist: %s", ident));
    assertFalse(dispatcher.semanticModelExists(ident));
  }

  @Test
  void testSemanticModelExistsPropagatesOtherFailures() {
    SemanticModelDispatcher dispatcher = mock(SemanticModelDispatcher.class, CALLS_REAL_METHODS);
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "model");
    IllegalStateException failure = new IllegalStateException("Unable to load Semantic Model");
    when(dispatcher.loadSemanticModel(ident)).thenThrow(failure);

    assertSame(
        failure,
        assertThrows(IllegalStateException.class, () -> dispatcher.semanticModelExists(ident)));
  }
}
