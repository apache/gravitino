/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.UserEntity;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/** Interface defining the operations for a Relation Backend. */
public interface RelationalBackend extends Closeable {

  /**
   * Initializes the Relational Backend environment with the provided configuration.
   *
   * @param config The configuration for the backend.
   */
  void initialize(Config config);

  /**
   * Lists the entities associated with the given parent namespace and entityType
   *
   * @param namespace The parent namespace of these entities.
   * @param entityType The type of these entities.
   * @return The list of entities associated with the given parent namespace and entityType, or null
   *     if the entities does not exist.
   * @throws NoSuchEntityException If the corresponding parent entity of these list entities cannot
   *     be found.
   */
  <E extends Entity & HasIdentifier> List<E> list(Namespace namespace, Entity.EntityType entityType)
      throws NoSuchEntityException;

  /**
   * Checks the entity associated with the given identifier and entityType whether exists.
   *
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @return True, if the entity can be found, else return false.
   */
  boolean exists(NameIdentifier ident, Entity.EntityType entityType);

  /**
   * Stores the entity, possibly overwriting an existing entity if specified.
   *
   * @param e The entity which need be stored.
   * @param overwritten If true, overwrites the existing value.
   * @throws EntityAlreadyExistsException If the entity already exists and overwrite is false.
   */
  <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException;

  /**
   * Updates the entity.
   *
   * @param ident The identifier of the entity which need be stored.
   * @param entityType The type of the entity.
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
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @return The entity associated with the identifier and the entity type, or null if the key does
   *     not exist.
   * @throws IOException If an I/O exception occurs during retrieval.
   */
  <E extends Entity & HasIdentifier> E get(NameIdentifier ident, Entity.EntityType entityType)
      throws IOException;

  /**
   * Soft deletes the entity associated with the identifier and the entity type.
   *
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @param cascade True, If you need to cascade delete entities, else false.
   * @return True, if the entity was successfully deleted, else false.
   */
  boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade);

  /**
   * Permanently deletes the legacy data that has been marked as deleted before the given legacy
   * timeline.
   *
   * @param entityType The type of the entity.
   * @param legacyTimeLine The time before which the data has been marked as deleted.
   * @return The count of the deleted data.
   */
  int hardDeleteLegacyData(Entity.EntityType entityType, long legacyTimeLine);

  /**
   * Soft deletes the old version data that is older than or equal to the given version retention
   * count.
   *
   * @param entityType The type of the entity.
   * @param versionRetentionCount The count of versions to retain.
   * @return The count of the deleted data.
   */
  int deleteOldVersionData(Entity.EntityType entityType, long versionRetentionCount);

  /**
   * Fetch an external fileset name the storage location of a new external fileset is already
   * mounted.
   *
   * @param storageLocation the storage location of a new external fileset
   * @return the fileset name for the storage location mounted
   */
  String fetchExternalFilesetName(String storageLocation);

  /**
   * Lists all UserEntity that has the role.
   *
   * @param ident the unique identifier of the role entity
   * @return the list of UserEntity
   */
  List<UserEntity> listUsersByRole(NameIdentifier ident);

  /**
   * Lists all GroupEntity that has the role.
   *
   * @param ident the unique identifier of the role entity
   * @return the list of GroupEntity
   */
  List<GroupEntity> listGroupsByRole(NameIdentifier ident);
}