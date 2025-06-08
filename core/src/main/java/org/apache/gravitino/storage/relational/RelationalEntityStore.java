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

import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.tag.SupportsTagOperations;
import org.apache.gravitino.utils.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relation store to store entities. This means we can store entities in a relational store. I.e.,
 * MySQL, PostgreSQL, etc. If you want to use a different backend, you can implement the {@link
 * RelationalBackend} interface
 */
public class RelationalEntityStore
    implements EntityStore, SupportsTagOperations, SupportsRelationOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationalEntityStore.class);
  public static final ImmutableMap<String, String> RELATIONAL_BACKENDS =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_RELATIONAL_STORE, JDBCBackend.class.getCanonicalName());
  private RelationalBackend backend;
  private RelationalGarbageCollector garbageCollector;
  private EntityCache cache;
  private boolean cacheEnabled;

  /**
   * Return whether the cache is enabled or not.
   *
   * @return {@code true} if cache is enable, otherwise {@code false}
   */
  public boolean cacheEnabled() {
    return cacheEnabled;
  }

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = createRelationalEntityBackend(config);
    this.garbageCollector = new RelationalGarbageCollector(backend, config);
    this.garbageCollector.start();
    // TODO USE SPI to load the cache
    this.cache = new CaffeineEntityCache(config);
    this.cacheEnabled = config.get(Configs.CACHE_ENABLED);
  }

  private static RelationalBackend createRelationalEntityBackend(Config config) {
    String backendName = config.get(ENTITY_RELATIONAL_STORE);
    String className =
        RELATIONAL_BACKENDS.getOrDefault(backendName, Configs.DEFAULT_ENTITY_RELATIONAL_STORE);

    try {
      RelationalBackend relationalBackend =
          (RelationalBackend) Class.forName(className).getDeclaredConstructor().newInstance();
      relationalBackend.initialize(config);
      return relationalBackend;
    } catch (Exception e) {
      LOGGER.error(
          "Failed to create and initialize RelationalBackend by name '{}'.", backendName, e);
      throw new RuntimeException(
          "Failed to create and initialize RelationalBackend by name: " + backendName, e);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType) throws IOException {
    if (!cacheEnabled) {
      return backend.list(namespace, entityType, false);
    }

    return cache.withCacheLock(
        () -> {
          List<E> entities = backend.list(namespace, entityType, false);
          entities.forEach(cache::put);

          return entities;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType, boolean allFields)
      throws IOException {
    if (!cacheEnabled) {
      return backend.list(namespace, entityType, allFields);
    }

    return cache.withCacheLock(
        () -> {
          List<E> entities = backend.list(namespace, entityType, allFields);
          entities.forEach(cache::put);

          return entities;
        });
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    if (!cacheEnabled) {
      return backend.exists(ident, entityType);
    }

    return cache.withCacheLock(
        () -> {
          if (cache.contains(ident, entityType)) {
            return true;
          }

          return backend.exists(ident, entityType);
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    if (!cacheEnabled) {
      backend.insert(e, overwritten);
      return;
    }

    cache.withCacheLock(
        () -> {
          backend.insert(e, overwritten);

          if (e.type() == Entity.EntityType.MODEL_VERSION) {
            NameIdentifier modelIdent = ((ModelVersionEntity) e).modelIdentifier();
            cache.invalidate(modelIdent, Entity.EntityType.MODEL);
          }
          cache.put(e);
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    if (!cacheEnabled) {
      return backend.update(ident, entityType, updater);
    }

    return cache.withCacheLock(
        () -> {
          cache.invalidate(ident, entityType);
          E updatedEntity = backend.update(ident, entityType, updater);
          cache.put(updatedEntity);

          return updatedEntity;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    if (!cacheEnabled) {
      return backend.get(ident, entityType);
    }

    return cache.withCacheLock(
        () -> {
          Optional<E> entityFromCache = cache.getIfPresent(ident, entityType);
          if (entityFromCache.isPresent()) {
            return entityFromCache.get();
          }

          E entity = backend.get(ident, entityType);
          cache.put(entity);
          return entity;
        });
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    try {
      if (!cacheEnabled) {
        return backend.delete(ident, entityType, cascade);
      }

      return cache.withCacheLock(
          () -> {
            cache.invalidate(ident, entityType);
            return backend.delete(ident, entityType, cascade);
          });
    } catch (NoSuchEntityException nse) {
      return false;
    }
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) {
    throw new UnsupportedOperationException("Unsupported operation in relational entity store.");
  }

  @Override
  public void close() throws IOException {
    garbageCollector.close();
    backend.close();
    cache.clear();
  }

  @Override
  public SupportsTagOperations tagOperations() {
    return this;
  }

  @Override
  public SupportsRelationOperations relationOperations() {
    return this;
  }

  @Override
  public List<MetadataObject> listAssociatedMetadataObjectsForTag(NameIdentifier tagIdent)
      throws IOException {
    return backend.listAssociatedMetadataObjectsForTag(tagIdent);
  }

  @Override
  public List<TagEntity> listAssociatedTagsForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType)
      throws NoSuchEntityException, IOException {
    return backend.listAssociatedTagsForMetadataObject(objectIdent, objectType);
  }

  @Override
  public TagEntity getTagForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType, NameIdentifier tagIdent)
      throws NoSuchEntityException, IOException {
    return backend.getTagForMetadataObject(objectIdent, objectType, tagIdent);
  }

  @Override
  public List<TagEntity> associateTagsWithMetadataObject(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      NameIdentifier[] tagsToAdd,
      NameIdentifier[] tagsToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException {
    return backend.associateTagsWithMetadataObject(
        objectIdent, objectType, tagsToAdd, tagsToRemove);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      Type relType, NameIdentifier nameIdentifier, Entity.EntityType identType, boolean allFields)
      throws IOException {
    if (!cacheEnabled) {
      return backend.listEntitiesByRelation(relType, nameIdentifier, identType, allFields);
    }

    return cache.withCacheLock(
        () -> {
          Optional<List<E>> entities = cache.getIfPresent(relType, nameIdentifier, identType);
          if (entities.isPresent()) {
            return entities.get();
          }

          List<E> EntitiesFromDb =
              backend.listEntitiesByRelation(relType, nameIdentifier, identType, allFields);

          if (!EntitiesFromDb.isEmpty()) {
            cache.put(nameIdentifier, identType, relType, EntitiesFromDb);
          }

          return EntitiesFromDb;
        });
  }

  @Override
  public void insertRelation(
      SupportsRelationOperations.Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override)
      throws IOException {
    if (!cacheEnabled) {
      backend.insertRelation(relType, srcIdentifier, srcType, dstIdentifier, dstType, override);
      return;
    }
    cache.withCacheLock(
        () -> {
          cache.invalidate(srcIdentifier, srcType, relType);
          backend.insertRelation(relType, srcIdentifier, srcType, dstIdentifier, dstType, override);
        });
  }
}
