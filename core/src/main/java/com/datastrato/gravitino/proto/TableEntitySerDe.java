/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.meta.TableEntity;

public class TableEntitySerDe implements ProtoSerDe<TableEntity, Table> {
  @Override
  public Table serialize(TableEntity tableEntity) {
    return Table.newBuilder()
        .setId(tableEntity.id())
        .setName(tableEntity.name())
        .setAuditInfo(new AuditInfoSerDe().serialize(tableEntity.auditInfo()))
        .build();
  }

  @Override
  public TableEntity deserialize(Table p, Namespace namespace) {
    return TableEntity.builder()
        .withId(p.getId())
        .withName(p.getName())
        .withNamespace(namespace)
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo(), namespace))
        .build();
  }
}
