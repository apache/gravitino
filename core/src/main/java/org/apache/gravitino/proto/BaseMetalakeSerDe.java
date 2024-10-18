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

/** A class for serializing and deserializing BaseMetalake objects. */
class BaseMetalakeSerDe implements ProtoSerDe<org.apache.gravitino.meta.BaseMetalake, Metalake> {

  /**
   * Serializes a {@link org.apache.gravitino.meta.BaseMetalake} object to a {@link Metalake}
   * object.
   *
   * @param baseMetalake The BaseMetalake object to be serialized.
   * @return The serialized Metalake object.
   */
  @Override
  public Metalake serialize(org.apache.gravitino.meta.BaseMetalake baseMetalake) {
    Metalake.Builder builder =
        Metalake.newBuilder()
            .setId(baseMetalake.id())
            .setName(baseMetalake.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(baseMetalake.auditInfo()));

    if (baseMetalake.comment() != null) {
      builder.setComment(baseMetalake.comment());
    }

    if (baseMetalake.properties() != null && !baseMetalake.properties().isEmpty()) {
      builder.putAllProperties(baseMetalake.properties());
    }

    SchemaVersion version =
        SchemaVersion.newBuilder()
            .setMajorNumber(baseMetalake.getVersion().majorVersion)
            .setMinorNumber(baseMetalake.getVersion().minorVersion)
            .build();
    builder.setVersion(version);

    return builder.build();
  }

  /**
   * Deserializes a {@link Metalake} object to a {@link org.apache.gravitino.meta.BaseMetalake}
   * object.
   *
   * @param p The serialized Metalake object.
   * @return The deserialized BaseMetalake object.
   */
  @Override
  public org.apache.gravitino.meta.BaseMetalake deserialize(Metalake p, Namespace namespace) {
    org.apache.gravitino.meta.BaseMetalake.Builder builder =
        org.apache.gravitino.meta.BaseMetalake.builder();
    builder
        .withId(p.getId())
        .withName(p.getName())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo(), namespace));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    org.apache.gravitino.meta.SchemaVersion version =
        org.apache.gravitino.meta.SchemaVersion.forValues(
            p.getVersion().getMajorNumber(), p.getVersion().getMinorNumber());
    builder.withVersion(version);

    return builder.build();
  }
}
