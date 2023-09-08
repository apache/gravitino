/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.proto;

import com.datastrato.graviton.meta.rel.BaseTable;

public class TableEntitySerde implements ProtoSerDe<BaseTable, Table> {
  @Override
  public Table serialize(BaseTable tableEntity) {
    return Table.newBuilder()
        .setId(tableEntity.id())
        .setName(tableEntity.name())
        .setAuditInfo(new AuditInfoSerDe().serialize(tableEntity.auditInfo()))
        .build();
  }

  @Override
  public BaseTable deserialize(Table p) {
    return new BaseTable.TableBuilder()
        .withId(p.getId())
        .withName(p.getName())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()))
        .build();
  }
}
