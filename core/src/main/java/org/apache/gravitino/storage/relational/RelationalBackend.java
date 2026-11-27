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
package org.apache.gravitino.storage.relational;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsDomainOperations;
import org.apache.gravitino.SupportsOntologyIdLookup;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.SupportsTableIdLookup;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.semantic.SemanticModelRevision;
import org.apache.gravitino.semantic.SemanticModelRevisionSelector;

/** Interface defining the operations for a Relation Backend. */
public interface RelationalBackend
    extends Closeable,
        SupportsRelationOperations,
        SupportsDomainOperations,
        SupportsTableIdLookup,
        SupportsOntologyIdLookup {

  /**
   * Resolves a table ID, retaining full reads for backends without a lightweight implementation.
   *
   * @param identifier the full table identifier
   * @return the active table ID
   * @throws NoSuchEntityException if the table does not exist
   * @throws IOException if the lookup fails
   */
  @Override
  default long getTableId(NameIdentifier identifier) throws IOException {
    return get(identifier, Entity.EntityType.TABLE).id();
  }

  /**
   * Resolves a Semantic Model ID from its identity, whether or not it has an active revision.
   *
   * <p>The default reads the active revision. A backend that supports deferred publication must
   * override it so that a model with only drafts still resolves.
   *
   * @param identifier the full Semantic Model identifier
   * @return the stable Semantic Model ID
   * @throws NoSuchEntityException if the Semantic Model or its namespace does not exist
   * @throws IOException if the lookup fails
   */
  default long getSemanticModelId(NameIdentifier identifier) throws IOException {
    return this.<SemanticModelEntity>get(identifier, Entity.EntityType.SEMANTIC_MODEL).id();
  }

  /** Loads a retained Semantic Model snapshot or the latest saved revision. */
  default SemanticModelEntity loadSemanticModelRevision(
      NameIdentifier ident, SemanticModelRevisionSelector selector) throws IOException {
    throw new UnsupportedOperationException("Loading Semantic Model revisions is not supported");
  }

  /** Lists retained Semantic Model revision metadata newest first. */
  default SemanticModelRevision[] listSemanticModelRevisions(NameIdentifier ident)
      throws IOException {
    throw new UnsupportedOperationException("Listing Semantic Model revisions is not supported");
  }

  /** Publishes the latest Semantic Model revision. */
  default SemanticModelEntity publishSemanticModelRevision(
      NameIdentifier ident, int revision, String principal) throws IOException {
    throw new UnsupportedOperationException("Publishing Semantic Model revisions is not supported");
  }

  /** Lists model identities for governance, irrespective of publication state. */
  default List<NameIdentifier> listSemanticModelIdentifiers(Namespace namespace)
      throws IOException {
    return this.<SemanticModelEntity>list(namespace, Entity.EntityType.SEMANTIC_MODEL, false)
        .stream()
        .map(SemanticModelEntity::nameIdentifier)
        .toList();
  }

  /** Applies a Semantic Model update with an optional revision precondition. */
  default SemanticModelEntity updateSemanticModel(
      NameIdentifier ident,
      @Nullable Integer expectedRevision,
      boolean rename,
      Function<SemanticModelEntity, SemanticModelEntity> updater)
      throws IOException {
    return update(ident, Entity.EntityType.SEMANTIC_MODEL, updater);
  }

  /**
   * Resolves an ontology ID, retaining full reads for backends without a lightweight
   * implementation.
   *
   * @param identifier the ontology identifier
   * @return the active ontology ID
   * @throws NoSuchEntityException if the ontology does not exist
   * @throws IOException if the lookup fails
   */
  @Override
  default long getOntologyId(NameIdentifier identifier) throws IOException {
    return get(identifier, Entity.EntityType.ONTOLOGY).id();
  }

  /**
   * Reads metric identifiers in a schema without loading complete Semantic Models.
   *
   * @param namespace The full schema namespace.
   * @return Metric identifiers from one consistent statement or read snapshot.
   * @throws NoSuchEntityException If the schema does not exist.
   * @throws IOException If the read fails.
   * @throws UnsupportedOperationException If metric discovery is not supported.
   */
  default List<NameIdentifier> listSemanticModelMetrics(Namespace namespace) throws IOException {
    throw new UnsupportedOperationException("Metric listing is not supported");
  }

  /**
   * Reads complete metrics in a schema without loading unrelated definition fields.
   *
   * @param namespace The full schema namespace.
   * @return Decoded metrics from one consistent statement or read snapshot, without authorization
   *     filtering.
   * @throws NoSuchEntityException If the schema does not exist.
   * @throws IOException If the read fails.
   * @throws UnsupportedOperationException If metric discovery is not supported.
   */
  default List<SemanticModelMetric> listSemanticModelMetricInfos(Namespace namespace)
      throws IOException {
    throw new UnsupportedOperationException("Metric info listing is not supported");
  }

  /**
   * Initializes the Relational Backend environment with the provided configuration.
   *
   * @param config The configuration for the backend.
   */
  void initialize(Config config);

  /**
   * Lists all recorded descendant schemas of a schema, excluding the schema itself.
   *
   * @param identifier The schema identifier using the configured logical separator.
   * @return The descendant schemas in logical form.
   * @throws IOException If the schemas cannot be read.
   * @throws NoSuchEntityException If the schema does not exist.
   * @throws UnsupportedOperationException If descendant schema listing is not supported.
   */
  default List<SchemaEntity> listDescendantSchemas(NameIdentifier identifier) throws IOException {
    throw new UnsupportedOperationException("This store cannot list descendant schemas");
  }

  /**
   * Lists the entities associated with the given parent namespace and entityType
   *
   * @param <E> The entity type.
   * @param namespace The parent namespace of these entities.
   * @param entityType The type of these entities.
   * @param allFields Some fields may have a relatively high acquisition cost, EntityStore provide
   *     an optional setting to avoid fetching these high-cost fields to improve the performance. If
   *     true, the method will fetch all the fields, Otherwise, the method will fetch all the fields
   *     except for high-cost fields.
   * @return The list of entities associated with the given parent namespace and entityType, or null
   *     if the entities does not exist.
   * @throws NoSuchEntityException If the corresponding parent entity of these list entities cannot
   *     be found.
   * @throws IOException If the store operation fails
   */
  <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType, boolean allFields)
      throws NoSuchEntityException, IOException;

  /**
   * Checks the entity associated with the given identifier and entityType whether exists.
   *
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @return True, if the entity can be found, else return false.
   * @throws IOException If the store operation fails
   */
  boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException;

  /**
   * Stores the entity, possibly overwriting an existing entity if specified.
   *
   * <p>A schema overwrite must not replace a different active identity at the same name or move an
   * active identity from another catalog. Such conflicts require explicit reconciliation, even when
   * {@code overwritten} is true; an overwrite must not cascade-delete the old schema metadata.
   *
   * @param <E> The type of the entity returned.
   * @param e The entity which need be stored.
   * @param overwritten If true, overwrites the existing value.
   * @throws EntityAlreadyExistsException If the entity already exists and overwrite is false, or a
   *     schema overwrite conflicts with an active identity.
   * @throws IOException If the store operation fails
   */
  <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException, IOException;

  /**
   * Updates the entity.
   *
   * @param <E> The entity type.
   * @param ident The identifier of the entity which need be stored.
   * @param entityType The type of the entity.
   * @param updater A {@link Function} that takes the current entity instance and returns and
   *     returns the updated instance.
   * @return The entity after updating.
   * @throws NoSuchEntityException If the entity is not exist.
   * @throws IOException If the entity is failed to update.
   */
  <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException;

  /**
   * Retrieves the entity associated with the identifier and the entity type.
   *
   * @param <E> The type of the entity returned.
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @return The entity associated with the identifier and the entity type, or null if the key does
   *     not exist.
   * @throws IOException If an I/O exception occurs during retrieval.
   */
  <E extends Entity & HasIdentifier> E get(NameIdentifier ident, Entity.EntityType entityType)
      throws IOException;

  /**
   * Batch retrieves the entities associated with the identifiers and the entity type.
   *
   * @param <E> The type of the entity returned.
   * @param identifiers The identifiers of the entities.
   * @param entityType The type of the entity.
   * @return The entities associated with the identifiers and the entity type, or null if the key
   *     does not exist.
   */
  <E extends Entity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> identifiers, Entity.EntityType entityType);

  /**
   * Soft deletes the entity associated with the identifier and the entity type.
   *
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @param cascade True, If you need to cascade delete entities, else false.
   * @return True, if the entity was successfully deleted, else false.
   * @throws IOException If the store operation fails
   */
  boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException;

  /**
   * Deletes an entity and returns the snapshot used by that delete.
   *
   * <p>Backends with a native compare-and-set delete should override this method so the returned
   * entity and the deleted row are based on the same read.
   *
   * @param ident the identifier of the entity
   * @param entityType the entity type
   * @param clazz the concrete entity class
   * @param <E> the concrete entity type
   * @return the deleted entity, or empty when it did not exist
   * @throws IOException if the store operation fails
   */
  default <E extends Entity & HasIdentifier> Optional<E> deleteAndGet(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> clazz) throws IOException {
    return deleteAndGet(ident, entityType, clazz, EntityStore.noPostDeleteAction());
  }

  /**
   * Deletes an entity and runs an action against the deleted snapshot before commit when supported.
   *
   * @param ident the identifier of the entity
   * @param entityType the entity type
   * @param clazz the concrete entity class
   * @param postDeleteAction the action to run after deletion but before commit when supported
   * @param <E> the concrete entity type
   * @return the deleted entity, or empty when it did not exist
   * @throws IOException if the store operation fails
   */
  default <E extends Entity & HasIdentifier> Optional<E> deleteAndGet(
      NameIdentifier ident,
      Entity.EntityType entityType,
      Class<E> clazz,
      Consumer<E> postDeleteAction)
      throws IOException {
    if (postDeleteAction != EntityStore.NO_POST_DELETE_ACTION) {
      // This implementation can only run the action once the delete is committed, which is the
      // opposite of what the contract promises. Refusing is better than silently leaving the
      // caller with a committed delete and a failed cleanup.
      throw new UnsupportedOperationException(
          "This store cannot run a post-delete action while the delete can still be rolled back");
    }

    try {
      E entity = get(ident, entityType);
      if (!delete(ident, entityType, false)) {
        return Optional.empty();
      }
      postDeleteAction.accept(entity);
      return Optional.of(entity);
    } catch (NoSuchEntityException e) {
      return Optional.empty();
    }
  }

  /**
   * Deletes a schema and optionally its functions, then runs an action before commit.
   *
   * <p>Remaining children, including filesets, must prevent deletion. Function cleanup, schema
   * deletion, and the action share one metadata transaction.
   *
   * @param ident the schema identifier
   * @param cascadeFunctions whether to delete functions in the same metadata transaction
   * @param postDeleteAction the action to run against the deleted snapshot before commit
   * @return the deleted schema, or empty when it did not exist
   * @throws IOException if the store operation fails
   * @throws UnsupportedOperationException if the backend cannot honor this transaction boundary
   */
  default Optional<SchemaEntity> deleteSchemaAndGet(
      NameIdentifier ident, boolean cascadeFunctions, Consumer<SchemaEntity> postDeleteAction)
      throws IOException {
    throw new UnsupportedOperationException(
        "This backend cannot delete a schema with a pre-commit action");
  }

  /**
   * Deletes the entities in the specified namespace and entity type.
   *
   * @param entitiesToDelete The list of identifiers and their entity types to be deleted.
   * @param cascade True, If you need to cascade delete entities, else false.
   * @return The count of the deleted entities.
   * @throws IOException If the store operation fails
   */
  int batchDelete(List<Pair<NameIdentifier, Entity.EntityType>> entitiesToDelete, boolean cascade)
      throws IOException;

  /**
   * Stores a batch of entities, possibly overwriting existing entities if specified.
   *
   * @param entities The list of entities to be stored.
   * @param overwritten If true, overwrites existing entities with the same identifier.
   * @param <E> The type of the entities in the list.
   * @throws IOException If the store operation fails
   * @throws EntityAlreadyExistsException If an entity already exists and overwrite is false.
   */
  <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
      throws IOException, EntityAlreadyExistsException;

  /**
   * Permanently deletes the legacy data that has been marked as deleted before the given legacy
   * timeline.
   *
   * @param entityType The type of the entity.
   * @param legacyTimeline The time before which the data has been marked as deleted.
   * @return The count of the deleted data.
   * @throws IOException If the store operation fails
   */
  int hardDeleteLegacyData(Entity.EntityType entityType, long legacyTimeline) throws IOException;

  /**
   * Soft deletes the old version data that is older than or equal to the given version retention
   * count.
   *
   * @param entityType The type of the entity.
   * @param versionRetentionCount The count of versions to retain.
   * @return The count of the deleted data.
   * @throws IOException If the store operation fails
   */
  int deleteOldVersionData(Entity.EntityType entityType, long versionRetentionCount)
      throws IOException;
}
