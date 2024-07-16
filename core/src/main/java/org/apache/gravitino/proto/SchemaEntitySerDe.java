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
import org.apache.gravitino.meta.SchemaEntity;

public class SchemaEntitySerDe implements ProtoSerDe<SchemaEntity, Schema> {
  @Override
  public Schema serialize(SchemaEntity schemaEntity) {
    Schema.Builder builder =
        Schema.newBuilder()
            .setId(schemaEntity.id())
            .setName(schemaEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(schemaEntity.auditInfo()));

    if (schemaEntity.comment() != null) {
      builder.setComment(schemaEntity.comment());
    }

    if (schemaEntity.properties() != null && !schemaEntity.properties().isEmpty()) {
      builder.putAllProperties(schemaEntity.properties());
    }

    return builder.build();
  }

  @Override
  public SchemaEntity deserialize(Schema p, Namespace namespace) {
    SchemaEntity.Builder builder =
        SchemaEntity.builder()
            .withId(p.getId())
            .withName(p.getName())
            .withNamespace(namespace)
            .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo(), namespace));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    return builder.build();
  }
}
