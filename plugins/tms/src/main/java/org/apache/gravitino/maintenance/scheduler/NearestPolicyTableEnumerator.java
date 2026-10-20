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
package org.apache.gravitino.maintenance.scheduler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.storage.relational.service.PolicyTagRelService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/**
 * Lists tables for which {@code policy} is the nearest maintenance policy of its type.
 *
 * <p>Walks {@code policy → tags → metadata objects}, expands catalog/schema attachments to tables,
 * then keeps a table only when {@link NearestMaintenancePolicyResolver} selects this policy id
 * (design: nearest attachment wins per {@link Policy.BuiltInType}).
 */
public final class NearestPolicyTableEnumerator implements PolicyTableEnumerator {

  private final PolicyTagRelService policyTagRelService;
  private final TagMetaService tagMetaService;
  private final SchemaMetaService schemaMetaService;
  private final TableMetaService tableMetaService;
  private final Supplier<EntityStore> entityStoreSupplier;

  /** Uses relational singletons and {@link GravitinoEnv#entityStore()}. */
  public NearestPolicyTableEnumerator() {
    this(
        PolicyTagRelService.getInstance(),
        TagMetaService.getInstance(),
        SchemaMetaService.getInstance(),
        TableMetaService.getInstance(),
        () -> GravitinoEnv.getInstance().entityStore());
  }

  /**
   * @param policyTagRelService policy-tag relations
   * @param tagMetaService tag metadata
   * @param schemaMetaService schema listing
   * @param tableMetaService table listing
   * @param entityStoreSupplier entity store for nearest-policy resolution
   */
  public NearestPolicyTableEnumerator(
      PolicyTagRelService policyTagRelService,
      TagMetaService tagMetaService,
      SchemaMetaService schemaMetaService,
      TableMetaService tableMetaService,
      Supplier<EntityStore> entityStoreSupplier) {
    this.policyTagRelService = policyTagRelService;
    this.tagMetaService = tagMetaService;
    this.schemaMetaService = schemaMetaService;
    this.tableMetaService = tableMetaService;
    this.entityStoreSupplier = entityStoreSupplier;
  }

  @Override
  public List<Long> listTableIds(PolicyEntity policy, int limit) {
    if (limit <= 0) {
      return List.of();
    }
    Policy.BuiltInType policyType = policy.policyType();
    String metalake = policy.namespace().level(0);
    NameIdentifier policyIdent = NameIdentifierUtil.ofPolicy(metalake, policy.name());
    List<RelationalEntity<?>> tagEdges =
        policyTagRelService.listRelations(List.of(policyIdent), Entity.EntityType.POLICY);

    NearestMaintenancePolicyResolver nearest =
        new NearestMaintenancePolicyResolver(entityStoreSupplier.get());
    Set<Long> seenTables = new LinkedHashSet<>();
    Set<Long> winners = new LinkedHashSet<>();

    for (RelationalEntity<?> edge : tagEdges) {
      Object target = edge.targetEntity();
      if (!(target instanceof TagEntity)) {
        continue;
      }
      TagEntity tag = (TagEntity) target;
      List<GenericEntity> objects;
      try {
        objects = tagMetaService.listAssociatedMetadataObjectsForTag(tag.nameIdentifier());
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to list objects for tag " + tag.nameIdentifier(), e);
      }
      for (GenericEntity object : objects) {
        if (winners.size() >= limit) {
          return new ArrayList<>(winners);
        }
        collectCandidates(
            metalake, object, policy.id(), policyType, nearest, seenTables, winners, limit);
      }
    }
    return new ArrayList<>(winners);
  }

  private void collectCandidates(
      String metalake,
      GenericEntity object,
      long policyId,
      Policy.BuiltInType policyType,
      NearestMaintenancePolicyResolver nearest,
      Set<Long> seenTables,
      Set<Long> winners,
      int limit) {
    MetadataObject.Type objectType;
    try {
      objectType = MetadataObject.Type.valueOf(object.type().name());
    } catch (IllegalArgumentException e) {
      return;
    }
    if (object.name() == null || object.name().isBlank()) {
      return;
    }
    MetadataObject metadataObject;
    try {
      metadataObject = MetadataObjects.parse(object.name(), objectType);
    } catch (RuntimeException e) {
      return;
    }

    switch (objectType) {
      case TABLE:
        if (object.id() == null) {
          return;
        }
        considerTable(
            object.id(),
            metadataObject,
            metalake,
            policyId,
            policyType,
            nearest,
            seenTables,
            winners);
        break;
      case SCHEMA:
        expandSchema(
            metalake, metadataObject, policyId, policyType, nearest, seenTables, winners, limit);
        break;
      case CATALOG:
        expandCatalog(
            metalake, metadataObject, policyId, policyType, nearest, seenTables, winners, limit);
        break;
      default:
        break;
    }
  }

  private void expandCatalog(
      String metalake,
      MetadataObject catalog,
      long policyId,
      Policy.BuiltInType policyType,
      NearestMaintenancePolicyResolver nearest,
      Set<Long> seenTables,
      Set<Long> winners,
      int limit) {
    List<SchemaEntity> schemas =
        schemaMetaService.listSchemasByNamespace(NamespaceUtil.ofSchema(metalake, catalog.name()));
    for (SchemaEntity schema : schemas) {
      if (winners.size() >= limit) {
        return;
      }
      MetadataObject schemaObject =
          MetadataObjects.of(catalog.name(), schema.name(), MetadataObject.Type.SCHEMA);
      expandSchema(
          metalake, schemaObject, policyId, policyType, nearest, seenTables, winners, limit);
    }
  }

  private void expandSchema(
      String metalake,
      MetadataObject schema,
      long policyId,
      Policy.BuiltInType policyType,
      NearestMaintenancePolicyResolver nearest,
      Set<Long> seenTables,
      Set<Long> winners,
      int limit) {
    String catalogName = schema.parent();
    if (catalogName == null) {
      return;
    }
    List<TableEntity> tables =
        tableMetaService.listTablesByNamespace(
            NamespaceUtil.ofTable(metalake, catalogName, schema.name()));
    for (TableEntity table : tables) {
      if (winners.size() >= limit) {
        return;
      }
      MetadataObject tableObject =
          NameIdentifierUtil.toMetadataObject(table.nameIdentifier(), Entity.EntityType.TABLE);
      considerTable(
          table.id(), tableObject, metalake, policyId, policyType, nearest, seenTables, winners);
    }
  }

  private static void considerTable(
      long tableId,
      MetadataObject tableObject,
      String metalake,
      long policyId,
      Policy.BuiltInType policyType,
      NearestMaintenancePolicyResolver nearest,
      Set<Long> seenTables,
      Set<Long> winners) {
    if (tableId <= 0 || !seenTables.add(tableId)) {
      return;
    }
    if (nearest.winningPolicyId(metalake, tableObject, policyType).orElse(-1L) == policyId) {
      winners.add(tableId);
    }
  }
}
