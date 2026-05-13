/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.storage.relational;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.exceptions.NoSuchEntityException;

/**
 * JDBC-backed {@link RelationalBackend} that translates hierarchical schema logical naming (API) to
 * the physical encoding expected by relational meta services, delegating storage operations to
 * {@link JDBCBackend}.
 *
 * <p>{@link JDBCBackend} is treated as storage-oriented: identifiers and entity payloads passed to
 * it use physical schema segments and embedded namespaces where applicable. Naming rules live in
 * {@link RelationalSchemaNamingBridge}, especially {@link
 * RelationalSchemaNamingBridge#entityForApi} (covers relation list elements) and {@link
 * RelationalSchemaNamingBridge#nameIdentifierForStorage}, so this class does not branch on {@link
 * Type} or {@link Entity.EntityType} for relation calls.
 */
public class HierarchicalSchemaRelationalBackend extends JDBCBackend {

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType, boolean allFields) throws IOException {
    Namespace storageParentNs = RelationalSchemaNamingBridge.embeddedNamespaceForStorage(namespace);
    List<E> raw = super.list(storageParentNs, entityType, allFields);
    return raw.stream()
        .map(e -> (E) RelationalSchemaNamingBridge.entityForApi(e, entityType))
        .collect(Collectors.toList());
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException, IOException {
    super.insert(RelationalSchemaNamingBridge.entityForStorage(e), overwritten);
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    return (E)
        RelationalSchemaNamingBridge.entityForApi(
            super.update(
                RelationalSchemaNamingBridge.nameIdentifierForStorage(ident, entityType),
                entityType,
                RelationalSchemaNamingBridge.wrapperUpdater(entityType, updater)),
            entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType)
      throws NoSuchEntityException, IOException {
    return (E)
        RelationalSchemaNamingBridge.entityForApi(
            super.get(
                RelationalSchemaNamingBridge.nameIdentifierForStorage(ident, entityType),
                entityType),
            entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> identifiers, Entity.EntityType entityType) {
    List<NameIdentifier> storageIdents =
        identifiers.stream()
            .map(id -> RelationalSchemaNamingBridge.nameIdentifierForStorage(id, entityType))
            .collect(Collectors.toList());
    List<E> got = super.batchGet(storageIdents, entityType);
    return got.stream()
        .map(e -> (E) RelationalSchemaNamingBridge.entityForApi(e, entityType))
        .collect(Collectors.toList());
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    return super.delete(
        RelationalSchemaNamingBridge.nameIdentifierForStorage(ident, entityType),
        entityType,
        cascade);
  }

  @Override
  public int batchDelete(
      List<Pair<NameIdentifier, Entity.EntityType>> entitiesToDelete, boolean cascade)
      throws IOException {
    List<Pair<NameIdentifier, Entity.EntityType>> storagePairs =
        entitiesToDelete.stream()
            .map(
                p ->
                    Pair.of(
                        RelationalSchemaNamingBridge.nameIdentifierForStorage(
                            p.getLeft(), p.getRight()),
                        p.getRight()))
            .collect(Collectors.toList());
    return super.batchDelete(storagePairs, cascade);
  }

  @Override
  public <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    List<E> storage =
        entities.stream()
            .map(RelationalSchemaNamingBridge::entityForStorage)
            .collect(Collectors.toList());
    super.batchPut(storage, overwritten);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      Type relType, NameIdentifier nameIdentifier, Entity.EntityType identType, boolean allFields)
      throws IOException {
    NameIdentifier source =
        RelationalSchemaNamingBridge.nameIdentifierForStorage(nameIdentifier, identType);
    List<E> out = super.listEntitiesByRelation(relType, source, identType, allFields);
    return out.stream()
        .map(e -> (E) RelationalSchemaNamingBridge.entityForApi(e, e.type()))
        .collect(Collectors.toList());
  }

  @Override
  public List<RelationalEntity<?>> batchListEntitiesByRelation(
      Type relType, List<NameIdentifier> nameIdentifiers, Entity.EntityType identType)
      throws IOException {
    List<NameIdentifier> storageIdents =
        nameIdentifiers.stream()
            .map(n -> RelationalSchemaNamingBridge.nameIdentifierForStorage(n, identType))
            .collect(Collectors.toList());
    Map<NameIdentifier, NameIdentifier> storageToLogical = new HashMap<>();
    for (int i = 0; i < nameIdentifiers.size(); i++) {
      storageToLogical.put(storageIdents.get(i), nameIdentifiers.get(i));
    }
    List<RelationalEntity<?>> raw =
        super.batchListEntitiesByRelation(relType, storageIdents, identType);
    return raw.stream()
        .map(re -> remapRelationEntity(re, storageToLogical))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static RelationalEntity<?> remapRelationEntity(
      RelationalEntity<?> re, Map<NameIdentifier, NameIdentifier> storageToLogical) {
    Entity tgt = re.targetEntity();
    return new RelationalEntity<>(
        re.type(),
        storageToLogical.get(re.source()),
        re.sourceType(),
            RelationalSchemaNamingBridge.entityForApi(
                (Entity & HasIdentifier) tgt, tgt.type()));
  }

  @Override
  public void insertRelation(
      Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override) {
    super.insertRelation(
        relType,
        RelationalSchemaNamingBridge.nameIdentifierForStorage(srcIdentifier, srcType),
        srcType,
        RelationalSchemaNamingBridge.nameIdentifierForStorage(dstIdentifier, dstType),
        dstType,
        override);
  }

  @Override
  public void batchInsertRelations(
      Type relType,
      List<NameIdentifier> srcIdentifiers,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override)
      throws IOException {
    List<NameIdentifier> srcStorage =
        srcIdentifiers.stream()
            .map(i -> RelationalSchemaNamingBridge.nameIdentifierForStorage(i, srcType))
            .collect(Collectors.toList());
    super.batchInsertRelations(
        relType,
        srcStorage,
        srcType,
        RelationalSchemaNamingBridge.nameIdentifierForStorage(dstIdentifier, dstType),
        dstType,
        override);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> updateEntityRelations(
      Type relType,
      NameIdentifier srcEntityIdent,
      Entity.EntityType srcEntityType,
      NameIdentifier[] destEntitiesToAdd,
      NameIdentifier[] destEntitiesToRemove)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    List<E> out =
        super.updateEntityRelations(
            relType,
            RelationalSchemaNamingBridge.nameIdentifierForStorage(srcEntityIdent, srcEntityType),
            srcEntityType,
            Arrays.stream(destEntitiesToAdd)
                .map(
                    id ->
                        RelationalSchemaNamingBridge.relationDestIdentifiersForStorage(relType, id))
                .toArray(NameIdentifier[]::new),
            Arrays.stream(destEntitiesToRemove)
                .map(
                    id ->
                        RelationalSchemaNamingBridge.relationDestIdentifiersForStorage(relType, id))
                .toArray(NameIdentifier[]::new));
    return out.stream()
        .map(e -> (E) RelationalSchemaNamingBridge.entityForApi(e, e.type()))
        .collect(Collectors.toList());
  }

  @Override
  public <E extends Entity & HasIdentifier> E getEntityByRelation(
      Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier destEntityIdent)
      throws IOException, NoSuchEntityException {
    E raw =
        super.getEntityByRelation(
            relType,
            RelationalSchemaNamingBridge.nameIdentifierForStorage(srcIdentifier, srcType),
            srcType,
            RelationalSchemaNamingBridge.relationDestIdentifiersForStorage(
                relType, destEntityIdent));
    return (E) RelationalSchemaNamingBridge.entityForApi(raw, raw.type());
  }
}
