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

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.SupportsTagOperations;
import org.apache.gravitino.utils.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cached Entity store, which caches metadata in memory. */
public class CachedEntityStore
    implements EntityStore, SupportsTagOperations, SupportsRelationOperations {
  private static final Logger LOG = LoggerFactory.getLogger(CachedEntityStore.class.getName());
  private final RelationalEntityStore entityStore;
  private final EntityCache cache;

  /**
   * Constructs a new instance of {@link CachedEntityStore}.
   *
   * @param entityStore The underlying entity store.
   * @param cache The {@link EntityCache} instance to use.
   */
  public CachedEntityStore(EntityStore entityStore, EntityCache cache) {
    this.entityStore = (RelationalEntityStore) entityStore;
    this.cache = cache;
  }

  /** {@inheritDoc} */
  @Override
  public void initialize(Config config) throws RuntimeException {
    // do nothing
  }

  /** {@inheritDoc} */
  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    return entityStore.exists(ident, entityType);
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    cache.withCacheLock(
        () -> {
          try {
            entityStore.put(e, overwritten);
          } catch (IOException ex) {
            LOG.error("Failed to put entity in entity store", ex);
            throw new RuntimeException(ex);
          }

          cache.put(e);
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    return cache.withCacheLock(
        () -> {
          cache.invalidate(ident, entityType);

          E updatedEntity;
          try {
            updatedEntity = entityStore.update(ident, type, entityType, updater);
          } catch (IOException e) {
            LOG.error("Failed to update entity in entity store", e);
            throw new RuntimeException(e);
          }
          cache.put(updatedEntity);

          return updatedEntity;
        });
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    return cache.getOrLoad(ident, entityType);
  }

  /** {@inheritDoc} */
  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    return cache.withCacheLock(
        () -> {
          cache.invalidate(ident, entityType);

          try {
            return entityStore.delete(ident, entityType, cascade);
          } catch (IOException e) {
            LOG.error("Failed to delete entity in entity store", e);
            throw new RuntimeException(e);
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException {
    return entityStore.executeInTransaction(executable);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // do nothing
    entityStore.close();
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      Type relType, NameIdentifier nameIdentifier, Entity.EntityType identType, boolean allFields)
      throws IOException {
    return entityStore.listEntitiesByRelation(relType, nameIdentifier, identType, allFields);
  }

  /** {@inheritDoc} */
  @Override
  public void insertRelation(
      Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override)
      throws IOException {
    entityStore.insertRelation(relType, srcIdentifier, srcType, dstIdentifier, dstType, override);
  }

  /** {@inheritDoc} */
  @Override
  public List<MetadataObject> listAssociatedMetadataObjectsForTag(NameIdentifier tagIdent)
      throws IOException {
    return entityStore.listAssociatedMetadataObjectsForTag(tagIdent);
  }

  /** {@inheritDoc} */
  @Override
  public List<TagEntity> listAssociatedTagsForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType)
      throws NoSuchEntityException, IOException {
    return entityStore.listAssociatedTagsForMetadataObject(objectIdent, objectType);
  }

  /** {@inheritDoc} */
  @Override
  public TagEntity getTagForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType, NameIdentifier tagIdent)
      throws NoSuchEntityException, IOException {
    return entityStore.getTagForMetadataObject(objectIdent, objectType, tagIdent);
  }

  /** {@inheritDoc} */
  @Override
  public List<TagEntity> associateTagsWithMetadataObject(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      NameIdentifier[] tagsToAdd,
      NameIdentifier[] tagsToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException {
    return entityStore.associateTagsWithMetadataObject(
        objectIdent, objectType, tagsToAdd, tagsToRemove);
  }
}
