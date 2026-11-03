/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.dispatcher;

import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records who created an object through the Iceberg REST catalog. The Iceberg catalog reports no
 * creator or create time, so an object imported into Gravitino after an IRC create would otherwise
 * have an empty audit.
 */
final class IcebergEntityAuditUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergEntityAuditUtils.class);

  private IcebergEntityAuditUtils() {}

  /**
   * Sets the creator and create time of an imported schema, table or view whose stored audit has no
   * creator. A creator the catalog already reported is kept. The object already exists when this is
   * called, so a failure to record the audit is logged rather than failing the request.
   *
   * @param identifier the Gravitino identifier of the object
   * @param type {@link Entity.EntityType#SCHEMA}, {@link Entity.EntityType#TABLE} or {@link
   *     Entity.EntityType#VIEW}
   * @param user the authenticated user who created the object
   */
  static void recordCreator(NameIdentifier identifier, Entity.EntityType type, String user) {
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    if (store == null) {
      return;
    }
    Instant createTime = Instant.now();
    try {
      switch (type) {
        case SCHEMA:
          store.update(
              identifier,
              SchemaEntity.class,
              type,
              schema -> withCreator(schema, withCreator(schema.auditInfo(), user, createTime)));
          break;
        case TABLE:
          store.update(
              identifier,
              TableEntity.class,
              type,
              table -> withCreator(table, withCreator(table.auditInfo(), user, createTime)));
          break;
        case VIEW:
          store.update(
              identifier,
              ViewEntity.class,
              type,
              view -> withCreator(view, withCreator(view.auditInfo(), user, createTime)));
          break;
        default:
          throw new IllegalArgumentException("Unsupported entity type: " + type);
      }
    } catch (Exception e) {
      LOG.warn("Failed to record the creator of {} {} created by {}", type, identifier, user, e);
    }
  }

  @VisibleForTesting
  static AuditInfo withCreator(AuditInfo audit, String user, Instant createTime) {
    if (audit != null && audit.creator() != null) {
      return audit;
    }
    return AuditInfo.builder()
        .withCreator(user)
        .withCreateTime(createTime)
        .withLastModifier(audit == null ? null : audit.lastModifier())
        .withLastModifiedTime(audit == null ? null : audit.lastModifiedTime())
        .build();
  }

  private static SchemaEntity withCreator(SchemaEntity schema, AuditInfo audit) {
    return SchemaEntity.builder()
        .withId(schema.id())
        .withName(schema.name())
        .withNamespace(schema.namespace())
        .withComment(schema.comment())
        .withProperties(schema.properties())
        .withAuditInfo(audit)
        .build();
  }

  private static TableEntity withCreator(TableEntity table, AuditInfo audit) {
    return TableEntity.builder()
        .withId(table.id())
        .withName(table.name())
        .withNamespace(table.namespace())
        .withComment(table.comment())
        .withProperties(table.properties())
        .withColumns(table.columns())
        .withPartitioning(table.partitioning())
        .withDistribution(table.distribution())
        .withSortOrders(table.sortOrders())
        .withIndexes(table.indexes())
        .withAuditInfo(audit)
        .build();
  }

  private static ViewEntity withCreator(ViewEntity view, AuditInfo audit) {
    return ViewEntity.builder()
        .withId(view.id())
        .withName(view.name())
        .withNamespace(view.namespace())
        .withComment(view.comment())
        .withColumns(view.columns())
        .withRepresentations(view.representations())
        .withDefaultCatalog(view.defaultCatalog())
        .withDefaultSchema(view.defaultSchema())
        .withProperties(view.properties())
        .withAuditInfo(audit)
        .build();
  }
}
