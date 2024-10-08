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
import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntitySerDe;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
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

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = createRelationalEntityBackend(config);
    this.garbageCollector = new RelationalGarbageCollector(backend, config);
    this.garbageCollector.start();
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
  public void setSerDe(EntitySerDe entitySerDe) {
    throw new UnsupportedOperationException("Unsupported operation in relational entity store.");
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType) throws IOException {
    return backend.list(namespace, entityType, false);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType, boolean allFields)
      throws IOException {
    return backend.list(namespace, entityType, allFields);
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    return backend.exists(ident, entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    backend.insert(e, overwritten);
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    return backend.update(ident, entityType, updater);
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    return backend.get(ident, entityType);
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    try {
      return backend.delete(ident, entityType, cascade);
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
    return backend.listEntitiesByRelation(relType, nameIdentifier, identType, allFields);
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
    backend.insertRelation(relType, srcIdentifier, srcType, dstIdentifier, dstType, true);
  }
}
