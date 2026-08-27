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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.IdGenerator;

/**
 * Provides storage-level Semantic Model operations backed by Gravitino's {@link EntityStore}.
 *
 * <p>The framework establishes the managed-operation boundary. Semantic Model entity persistence
 * and lifecycle implementations will be added in a follow-up change.
 */
public class ManagedSemanticModelOperations implements SemanticModelCatalog {

  @SuppressWarnings("UnusedVariable")
  private final EntityStore store;

  @SuppressWarnings("UnusedVariable")
  private final IdGenerator idGenerator;

  /**
   * Creates managed Semantic Model operations.
   *
   * @param store The EntityStore used for persistence.
   * @param idGenerator The stable entity ID generator.
   */
  public ManagedSemanticModelOperations(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  @Override
  public NameIdentifier[] listSemanticModels(Namespace namespace) throws NoSuchSchemaException {
    // TODO: Implement when SemanticModelEntity is available.
    throw new UnsupportedOperationException(
        "listSemanticModels: SemanticModelEntity is not yet implemented");
  }

  @Override
  public SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException {
    // TODO: Implement when SemanticModelEntity is available.
    throw new UnsupportedOperationException(
        "loadSemanticModel: SemanticModelEntity is not yet implemented");
  }

  @Override
  public SemanticModel createSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    // TODO: Implement when SemanticModelEntity is available.
    throw new UnsupportedOperationException(
        "createSemanticModel: SemanticModelEntity is not yet implemented");
  }

  @Override
  public SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    // TODO: Implement when SemanticModelEntity is available.
    throw new UnsupportedOperationException(
        "alterSemanticModel: SemanticModelEntity is not yet implemented");
  }

  @Override
  public boolean dropSemanticModel(NameIdentifier ident) {
    // TODO: Implement when SemanticModelEntity is available.
    throw new UnsupportedOperationException(
        "dropSemanticModel: SemanticModelEntity is not yet implemented");
  }
}
