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
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;

/** A class for serializing and deserializing CatalogEntity objects using Protocol Buffers. */
public class CatalogEntitySerDe implements ProtoSerDe<CatalogEntity, Catalog> {

  /**
   * Serializes a {@link CatalogEntity} object to a {@link Catalog} object.
   *
   * @param catalogEntity The CatalogEntity object to be serialized.
   * @return The serialized Catalog object.
   */
  @Override
  public Catalog serialize(CatalogEntity catalogEntity) {
    Catalog.Builder builder =
        Catalog.newBuilder()
            .setId(catalogEntity.id())
            .setName(catalogEntity.name())
            .setProvider(catalogEntity.getProvider())
            .setAuditInfo(new AuditInfoSerDe().serialize((AuditInfo) catalogEntity.auditInfo()));

    if (catalogEntity.getComment() != null) {
      builder.setComment(catalogEntity.getComment());
    }

    if (catalogEntity.getProperties() != null && !catalogEntity.getProperties().isEmpty()) {
      builder.putAllProperties(catalogEntity.getProperties());
    }

    org.apache.gravitino.proto.Catalog.Type type =
        org.apache.gravitino.proto.Catalog.Type.valueOf(catalogEntity.getType().name());
    builder.setType(type);

    // Note we have ignored the namespace field here
    return builder.build();
  }

  /**
   * Deserializes a {@link Catalog} object to a {@link CatalogEntity} object.
   *
   * @param p The serialized Catalog object.
   * @return The deserialized CatalogEntity object.
   */
  @Override
  public CatalogEntity deserialize(Catalog p, Namespace namespace) {
    CatalogEntity.Builder builder = CatalogEntity.builder();
    builder
        .withId(p.getId())
        .withName(p.getName())
        .withNamespace(namespace)
        .withProvider(p.getProvider())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo(), namespace));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    builder.withType(org.apache.gravitino.Catalog.Type.valueOf(p.getType().name()));
    return builder.build();
  }
}
