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
import java.util.Arrays;
import java.util.Comparator;
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
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.semantic.SemanticModelSourceUpdate;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.gravitino.utils.PrincipalUtils;

/** EntityStore-backed operations for Gravitino-managed Semantic Models. */
public class ManagedSemanticModelOperations {

  private final EntityStore store;
  private final IdGenerator idGenerator;
  private final BiConsumer<NameIdentifier, SemanticModelDefinition> writeValidator;

  /**
   * Creates managed Semantic Model operations.
   *
   * @param store The EntityStore used for persistence.
   * @param idGenerator The stable entity ID generator.
   * @param writeValidator The definition validator for write operations.
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

  /**
   * Lists current model identifiers.
   *
   * @param namespace The schema namespace.
   * @return The model identifiers in the schema.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
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

  /**
   * Lists metric identifiers in structured name order.
   *
   * @param namespace The schema namespace.
   * @return Metric identifiers ordered by model identifier and case-sensitive metric name.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  public NameIdentifier[] listMetrics(Namespace namespace) throws NoSuchSchemaException {
    try {
      return store.listSemanticModelMetrics(namespace).stream()
          .sorted(
              Comparator.comparing(
                      (NameIdentifier ident) -> ident.namespace().levels(), Arrays::compare)
                  .thenComparing(NameIdentifier::name))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", namespace);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to list Semantic Model metrics in namespace " + namespace, e);
    }
  }

  /**
   * Lists complete metrics in structured name order.
   *
   * @param namespace The schema namespace.
   * @return Metrics ordered by model identifier and case-sensitive metric name.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  public SemanticModelMetric[] listMetricInfos(Namespace namespace) throws NoSuchSchemaException {
    try {
      return store.listSemanticModelMetricInfos(namespace).stream()
          .sorted(
              Comparator.comparing(
                      (SemanticModelMetric info) -> info.namespace().levels(), Arrays::compare)
                  .thenComparing(info -> info.metric().name()))
          .toArray(SemanticModelMetric[]::new);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", namespace);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to list Semantic Model metrics in namespace " + namespace, e);
    }
  }

  /**
   * Loads a current model.
   *
   * @param ident The model identifier.
   * @return The loaded model.
   * @throws NoSuchSemanticModelException If the model does not exist.
   */
  public SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException {
    try {
      return store.get(ident, Entity.EntityType.SEMANTIC_MODEL, SemanticModelEntity.class);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSemanticModelException(e, "Semantic Model %s does not exist", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load Semantic Model " + ident, e);
    }
  }

  /**
   * Creates a model with its validated source associations.
   *
   * @param ident The model identifier.
   * @param comment The model comment, or {@code null}.
   * @param definition The complete model definition.
   * @param properties The model properties.
   * @param sourceUpdate The resolved associations for every dataset in the definition.
   * @return The created model.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws SemanticModelAlreadyExistsException If the model already exists.
   * @throws IllegalSemanticModelException If the definition is invalid.
   */
  public SemanticModel createSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties,
      SemanticModelSourceUpdate sourceUpdate)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    Preconditions.checkArgument(properties != null, "Properties must not be null");

    Preconditions.checkArgument(
        sourceUpdate.replacesSources(), "Creation requires resolved source associations");
    writeValidator.accept(ident, definition);
    sourceUpdate.validate(definition);

    Instant now = Instant.now();
    SemanticModelEntity entity =
        SemanticModelEntity.builder()
            .withId(idGenerator.nextId())
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withDefinition(definition)
            .withSourceUpdate(sourceUpdate)
            .withProperties(properties)
            .withVersion(SemanticModelPO.INITIAL_VERSION)
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

  /**
   * Applies model changes and source association updates atomically.
   *
   * @param ident The model identifier.
   * @param sourceUpdate A replacement set for definition writes, or preserve for metadata changes.
   * @param changes The ordered changes to apply.
   * @return The updated model.
   * @throws NoSuchSemanticModelException If the model does not exist.
   * @throws SemanticModelAlreadyExistsException If a rename conflicts with an existing model.
   * @throws IllegalSemanticModelException If the changes or resulting definition are invalid.
   */
  public SemanticModel alterSemanticModel(
      NameIdentifier ident, SemanticModelSourceUpdate sourceUpdate, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    boolean validateForWrite = requiresWriteValidation(changes);
    Preconditions.checkArgument(
        validateForWrite == sourceUpdate.replacesSources(),
        "Definition replacement must replace sources; metadata-only changes must preserve sources");

    try {
      return store.update(
          ident,
          SemanticModelEntity.class,
          Entity.EntityType.SEMANTIC_MODEL,
          oldEntity -> {
            SemanticModelEntity candidate = applyChanges(oldEntity, sourceUpdate, changes);
            if (validateForWrite) {
              writeValidator.accept(
                  NameIdentifier.of(candidate.namespace(), candidate.name()),
                  candidate.definition());
            }
            sourceUpdate.validate(candidate.definition());
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

  /**
   * Deletes a model and retires its source associations.
   *
   * @param ident The model identifier.
   * @return Whether the model was deleted.
   */
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
      SemanticModelEntity oldEntity,
      SemanticModelSourceUpdate sourceUpdate,
      SemanticModelChange[] changes) {
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
        .withSourceUpdate(sourceUpdate)
        .withProperties(newProperties)
        .withVersion(oldEntity.version() + 1)
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
