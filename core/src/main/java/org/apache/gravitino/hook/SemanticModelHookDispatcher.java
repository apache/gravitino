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
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
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
  private final Supplier<OwnerDispatcher> ownerDispatcher;

  /**
   * Creates a Semantic Model hook dispatcher.
   *
   * @param dispatcher The underlying dispatcher.
   * @param ownerDispatcher Supplies the owner dispatcher, or null when authorization is disabled.
   */
  public SemanticModelHookDispatcher(
      SemanticModelDispatcher dispatcher, Supplier<OwnerDispatcher> ownerDispatcher) {
    this.dispatcher = dispatcher;
    this.ownerDispatcher = ownerDispatcher;
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
    OwnerDispatcher ownerManager = ownerDispatcher.get();
    if (ownerManager != null) {
      ownerManager.setOwner(
          ident.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.SEMANTIC_MODEL),
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
