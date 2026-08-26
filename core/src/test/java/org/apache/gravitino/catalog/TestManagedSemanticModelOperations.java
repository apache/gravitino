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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Test;

public class TestManagedSemanticModelOperations {

  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "schema");
  private static final NameIdentifier IDENT = NameIdentifier.of(NAMESPACE, "model");

  private final ManagedSemanticModelOperations operations =
      new ManagedSemanticModelOperations(mock(EntityStore.class), mock(IdGenerator.class));

  @Test
  public void testOperationsRemainUnsupportedUntilEntityPersistenceIsAdded() {
    SemanticModelDefinition definition = mock(SemanticModelDefinition.class);

    assertThrows(
        UnsupportedOperationException.class, () -> operations.listSemanticModels(NAMESPACE));
    assertThrows(UnsupportedOperationException.class, () -> operations.loadSemanticModel(IDENT));
    assertThrows(
        UnsupportedOperationException.class,
        () -> operations.createSemanticModel(IDENT, null, definition, Map.of()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> operations.alterSemanticModel(IDENT, SemanticModelChange.updateComment("updated")));
    assertThrows(UnsupportedOperationException.class, () -> operations.dropSemanticModel(IDENT));
  }
}
