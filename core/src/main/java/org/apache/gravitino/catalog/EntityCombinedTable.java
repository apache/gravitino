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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Audit;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/**
 * A Table class to represent a table metadata object that combines the metadata both from {@link
 * Table} and {@link TableEntity}.
 */
public final class EntityCombinedTable implements Table {

  private final Table table;

  private final TableEntity tableEntity;

  // Sets of properties that should be hidden from the user.
  private Set<String> hiddenProperties;

  // Field "imported" is used to indicate whether the entity has been imported to Gravitino
  // managed storage backend. If "imported" is true, it means that storage backend have stored
  // the correct entity. Otherwise, we should import the external entity to the storage backend.
  // This is used for tag/access control related purposes, only the imported entities have the
  // unique id, and based on this id, we can label and control the access to the entities.
  private boolean imported;

  private EntityCombinedTable(Table table, TableEntity tableEntity) {
    this.table = table;
    this.tableEntity = tableEntity;
    this.imported = false;
  }

  public static EntityCombinedTable of(Table table, TableEntity tableEntity) {
    return new EntityCombinedTable(table, tableEntity);
  }

  public static EntityCombinedTable of(Table table) {
    return new EntityCombinedTable(table, null);
  }

  public EntityCombinedTable withHiddenProperties(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties;
    return this;
  }

  public EntityCombinedTable withImported(boolean imported) {
    this.imported = imported;
    return this;
  }

  @Override
  public String name() {
    return table.name();
  }

  @Override
  public String comment() {
    return table.comment();
  }

  @Override
  public Column[] columns() {
    return table.columns();
  }

  @Override
  public Map<String, String> properties() {
    return table.properties().entrySet().stream()
        .filter(p -> !hiddenProperties.contains(p.getKey()))
        .filter(entry -> entry.getKey() != null && entry.getValue() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return table.supportPartitions();
  }

  @Override
  public Transform[] partitioning() {
    return table.partitioning();
  }

  @Override
  public SortOrder[] sortOrder() {
    return table.sortOrder();
  }

  @Override
  public Distribution distribution() {
    return table.distribution();
  }

  @Override
  public Index[] index() {
    return table.index();
  }

  public boolean imported() {
    return imported;
  }

  public Table tableFromCatalog() {
    return table;
  }

  public TableEntity tableFromGravitino() {
    return tableEntity;
  }

  @Override
  public Audit auditInfo() {
    if (tableEntity == null) {
      return table.auditInfo();
    }

    Audit catalogAudit = table.auditInfo();
    Audit entityAudit = tableEntity.auditInfo();

    // Entity store values take priority when non-null (they track actual Gravitino operations),
    // but fall back to catalog-provided values (e.g., owner from Iceberg metadata).
    return AuditInfo.builder()
        .withCreator(entityAudit.creator() != null ? entityAudit.creator() : catalogAudit.creator())
        .withCreateTime(
            entityAudit.createTime() != null ? entityAudit.createTime() : catalogAudit.createTime())
        .withLastModifier(
            entityAudit.lastModifier() != null
                ? entityAudit.lastModifier()
                : catalogAudit.lastModifier())
        .withLastModifiedTime(
            entityAudit.lastModifiedTime() != null
                ? entityAudit.lastModifiedTime()
                : catalogAudit.lastModifiedTime())
        .build();
  }

  StringIdentifier stringIdentifier() {
    return StringIdentifier.fromProperties(table.properties());
  }
}
