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
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;

/** EntityStore-backed operations for Gravitino-managed Semantic Models. */
public class ManagedSemanticModelOperations implements SemanticModelCatalog {

  private final EntityStore store;
  private final IdGenerator idGenerator;
  private final BiConsumer<NameIdentifier, SemanticModelDefinition> writeValidator;

  /**
   * Creates managed Semantic Model operations.
   *
   * @param store The EntityStore used for persistence.
   * @param idGenerator The stable entity ID generator.
   * @param writeValidator The complete definition and source validator for write operations.
   */
  public ManagedSemanticModelOperations(
      EntityStore store,
      IdGenerator idGenerator,
      BiConsumer<NameIdentifier, SemanticModelDefinition> writeValidator) {
    Preconditions.checkArgument(store != null, "EntityStore must not be null");
    Preconditions.checkArgument(idGenerator != null, "IdGenerator must not be null");
    Preconditions.checkArgument(writeValidator != null, "Write validator must not be null");
    this.store = store;
    this.idGenerator = idGenerator;
    this.writeValidator = writeValidator;
  }

  @Override
  public NameIdentifier[] listSemanticModels(Namespace namespace) throws NoSuchSchemaException {
    try {
      List<SemanticModelEntity> models =
          store.list(namespace, SemanticModelEntity.class, Entity.EntityType.SEMANTIC_MODEL);
      return models.stream()
          .map(model -> NameIdentifier.of(namespace, model.name()))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", namespace);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list Semantic Models in namespace " + namespace, e);
    }
  }

  @Override
  public SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException {
    try {
      return store.get(ident, Entity.EntityType.SEMANTIC_MODEL, SemanticModelEntity.class);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSemanticModelException(e, "Semantic Model %s does not exist", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load Semantic Model " + ident, e);
    }
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

    writeValidator.accept(ident, definition);

    Instant now = Instant.now();
    SemanticModelEntity entity =
        SemanticModelEntity.builder()
            .withId(idGenerator.nextId())
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withDefinition(definition)
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentUserName())
                    .withCreateTime(now)
                    .build())
            .build();

    try {
      store.put(entity, false /* overwrite */);
      return entity;
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", ident.namespace());
    } catch (EntityAlreadyExistsException e) {
      throw new SemanticModelAlreadyExistsException(e, "Semantic Model %s already exists", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Semantic Model " + ident, e);
    }
  }

  @Override
  public SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    boolean validateForWrite = requiresWriteValidation(changes);

    try {
      return store.update(
          ident,
          SemanticModelEntity.class,
          Entity.EntityType.SEMANTIC_MODEL,
          oldEntity -> {
            SemanticModelEntity candidate = applyChanges(oldEntity, changes);
            if (validateForWrite) {
              writeValidator.accept(
                  NameIdentifier.of(candidate.namespace(), candidate.name()),
                  candidate.definition());
            }
            return candidate;
          });
    } catch (NoSuchEntityException e) {
      throw new NoSuchSemanticModelException(e, "Semantic Model %s does not exist", ident);
    } catch (EntityAlreadyExistsException e) {
      throw new SemanticModelAlreadyExistsException(
          e, "A Semantic Model with the requested name already exists while altering %s", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to alter Semantic Model " + ident, e);
    }
  }

  @Override
  public boolean dropSemanticModel(NameIdentifier ident) {
    try {
      return store.delete(ident, Entity.EntityType.SEMANTIC_MODEL);
    } catch (NoSuchEntityException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException("Failed to drop Semantic Model " + ident, e);
    }
  }

  private boolean requiresWriteValidation(SemanticModelChange[] changes) {
    if (changes == null || changes.length == 0) {
      throw new IllegalSemanticModelException("At least one Semantic Model change is required");
    }

    boolean requiresWriteValidation = false;
    for (SemanticModelChange change : changes) {
      if (change == null) {
        throw new IllegalSemanticModelException("Semantic Model change must not be null");
      }

      if (change instanceof SemanticModelChange.ReplaceDefinition) {
        requiresWriteValidation = true;
      } else if (change instanceof SemanticModelChange.RenameSemanticModel
          || change instanceof SemanticModelChange.UpdateComment
          || change instanceof SemanticModelChange.SetProperty
          || change instanceof SemanticModelChange.RemoveProperty) {
        // These changes do not modify the definition and must not revisit its sources.
      } else {
        throw new IllegalSemanticModelException(
            "Unsupported Semantic Model change: %s", change.getClass().getName());
      }
    }
    return requiresWriteValidation;
  }

  private static SemanticModelEntity applyChanges(
      SemanticModelEntity oldEntity, SemanticModelChange[] changes) {
    String newName = oldEntity.name();
    String newComment = oldEntity.comment();
    Map<String, String> newProperties = new HashMap<>(oldEntity.properties());
    SemanticModelDefinition newDefinition = oldEntity.definition();

    for (SemanticModelChange change : changes) {
      if (change instanceof SemanticModelChange.RenameSemanticModel) {
        newName = ((SemanticModelChange.RenameSemanticModel) change).getNewName();
      } else if (change instanceof SemanticModelChange.UpdateComment) {
        newComment = ((SemanticModelChange.UpdateComment) change).getNewComment();
      } else if (change instanceof SemanticModelChange.SetProperty) {
        SemanticModelChange.SetProperty setProperty = (SemanticModelChange.SetProperty) change;
        newProperties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof SemanticModelChange.RemoveProperty) {
        newProperties.remove(((SemanticModelChange.RemoveProperty) change).getProperty());
      } else if (change instanceof SemanticModelChange.ReplaceDefinition) {
        newDefinition = ((SemanticModelChange.ReplaceDefinition) change).getDefinition();
      }
    }

    AuditInfo oldAudit = oldEntity.auditInfo();
    return SemanticModelEntity.builder()
        .withId(oldEntity.id())
        .withName(newName)
        .withNamespace(oldEntity.namespace())
        .withComment(newComment)
        .withDefinition(newDefinition)
        .withProperties(newProperties)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(oldAudit.creator())
                .withCreateTime(oldAudit.createTime())
                .withLastModifier(PrincipalUtils.getCurrentUserName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }
}
