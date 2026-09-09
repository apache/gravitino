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

import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.IdGenerator;

/** Dispatches always-managed Semantic Model operations to Gravitino's EntityStore. */
public class SemanticModelOperationDispatcher extends OperationDispatcher
    implements SemanticModelDispatcher {

  private final CatalogManager catalogManager;
  private final SchemaDispatcher schemaDispatcher;
  private final ManagedSemanticModelOperations managedOperations;

  /**
   * Creates a Semantic Model operation dispatcher.
   *
   * @param catalogManager The catalog manager.
   * @param schemaDispatcher The schema operation dispatcher used for parent validation.
   * @param tableDispatcher The internal table dispatcher used for source validation.
   * @param viewDispatcher The internal view dispatcher used for source validation.
   * @param store The EntityStore used for Semantic Model persistence.
   * @param idGenerator The stable entity ID generator.
   * @param secretManager The secret manager required by the operation dispatcher base class.
   */
  public SemanticModelOperationDispatcher(
      CatalogManager catalogManager,
      SchemaDispatcher schemaDispatcher,
      TableDispatcher tableDispatcher,
      ViewDispatcher viewDispatcher,
      EntityStore store,
      IdGenerator idGenerator,
      SecretManager secretManager) {
    super(catalogManager, store, idGenerator, secretManager);
    this.catalogManager = catalogManager;
    this.schemaDispatcher = schemaDispatcher;
    SemanticModelValidator validator =
        new SemanticModelValidator(catalogManager, tableDispatcher, viewDispatcher);
    this.managedOperations =
        new ManagedSemanticModelOperations(store, idGenerator, validator::validateForWrite);
  }

  @Override
  public NameIdentifier[] listSemanticModels(Namespace namespace) throws NoSuchSchemaException {
    checkRelationalCatalog(namespace);
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    schemaDispatcher.loadSchema(schemaIdent);
    return managedOperations.listSemanticModels(namespace);
  }

  @Override
  public SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException {
    checkRelationalCatalog(ident.namespace());
    if (!schemaDispatcher.schemaExists(schemaIdentifier(ident))) {
      throw new NoSuchSemanticModelException("Semantic Model %s does not exist", ident);
    }
    return managedOperations.loadSemanticModel(ident);
  }

  @Override
  public SemanticModel createSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    Preconditions.checkArgument(properties != null, "Properties must not be null");
    checkRelationalCatalog(ident.namespace());
    NameIdentifier schemaIdent = schemaIdentifier(ident);
    schemaDispatcher.loadSchema(schemaIdent);
    return managedOperations.createSemanticModel(ident, comment, definition, properties);
  }

  @Override
  public SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    checkRelationalCatalog(ident.namespace());
    NameIdentifier schemaIdent = schemaIdentifier(ident);
    if (!schemaDispatcher.schemaExists(schemaIdent)) {
      throw new NoSuchSemanticModelException("Semantic Model %s does not exist", ident);
    }
    return managedOperations.alterSemanticModel(ident, changes);
  }

  @Override
  public boolean dropSemanticModel(NameIdentifier ident) {
    checkRelationalCatalog(ident.namespace());
    if (!schemaDispatcher.schemaExists(schemaIdentifier(ident))) {
      return false;
    }
    return managedOperations.dropSemanticModel(ident);
  }

  private void checkRelationalCatalog(Namespace namespace) {
    NameIdentifier catalogIdent = NameIdentifier.of(namespace.level(0), namespace.level(1));
    Catalog catalog = catalogManager.loadCatalog(catalogIdent);
    if (catalog.type() != Catalog.Type.RELATIONAL) {
      throw new UnsupportedOperationException(
          String.format(
              "Catalog %s has type %s and does not support Semantic Model operations",
              catalogIdent, catalog.type()));
    }
  }

  private static NameIdentifier schemaIdentifier(NameIdentifier ident) {
    return NameIdentifier.of(ident.namespace().levels());
  }
}
