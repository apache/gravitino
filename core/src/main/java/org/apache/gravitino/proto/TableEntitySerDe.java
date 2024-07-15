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
package org.apache.gravitino.proto;

import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.TableEntity;

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
