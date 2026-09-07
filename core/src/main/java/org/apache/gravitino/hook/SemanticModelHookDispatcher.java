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

package org.apache.gravitino.hook;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code SemanticModelHookDispatcher} is a decorator for {@link SemanticModelDispatcher} that not
 * only delegates Semantic Model operations to the underlying dispatcher but also executes some hook
 * operations before or after the underlying operations.
 */
public class SemanticModelHookDispatcher implements SemanticModelDispatcher {

  private final SemanticModelDispatcher dispatcher;

  /**
   * Creates a Semantic Model hook dispatcher.
   *
   * @param dispatcher The underlying dispatcher.
   */
  public SemanticModelHookDispatcher(SemanticModelDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listSemanticModels(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listSemanticModels(namespace);
  }

  @Override
  public SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException {
    return dispatcher.loadSemanticModel(ident);
  }

  @Override
  public boolean semanticModelExists(NameIdentifier ident) {
    return dispatcher.semanticModelExists(ident);
  }

  @Override
  public SemanticModel createSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    SemanticModel semanticModel =
        dispatcher.createSemanticModel(ident, comment, definition, properties);

    // Set the creator as the owner of the Semantic Model.
    OwnerDispatcher ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerDispatcher != null) {
      // The inner NormalizeDispatcher case-folds the parent namespace based on catalog
      // capabilities, so the entity is stored under the normalized identifier. Rebuild that
      // identifier here - namespace from the catalog capability, name from the created Semantic
      // Model, which already carries the Gravitino-owned naming rules - so the owner is attached to
      // the same identifier the manager sees.
      Capability capability =
          CapabilityHelpers.getCapability(ident, GravitinoEnv.getInstance().catalogManager());
      Namespace normalizedNamespace =
          CapabilityHelpers.applyCapabilities(
              ident.namespace(), Capability.Scope.SEMANTIC_MODEL, capability);
      NameIdentifier normalizedIdent = NameIdentifier.of(normalizedNamespace, semanticModel.name());
      ownerDispatcher.setOwner(
          normalizedIdent.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(normalizedIdent, Entity.EntityType.SEMANTIC_MODEL),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return semanticModel;
  }

  @Override
  public SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    return dispatcher.alterSemanticModel(ident, changes);
  }

  @Override
  public boolean dropSemanticModel(NameIdentifier ident) {
    return dispatcher.dropSemanticModel(ident);
  }
}
