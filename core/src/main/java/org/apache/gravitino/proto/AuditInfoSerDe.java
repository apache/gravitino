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

import java.util.Optional;
import org.apache.gravitino.Namespace;

/** A class for serializing and deserializing AuditInfo objects. */
class AuditInfoSerDe implements ProtoSerDe<org.apache.gravitino.meta.AuditInfo, AuditInfo> {

  /**
   * Serializes an {@link org.apache.gravitino.meta.AuditInfo} object to a {@link AuditInfo} object.
   *
   * @param auditInfo The AuditInfo object to be serialized.
   * @return The serialized AuditInfo object.
   */
  @Override
  public AuditInfo serialize(org.apache.gravitino.meta.AuditInfo auditInfo) {
    AuditInfo.Builder builder = AuditInfo.newBuilder();

    Optional.ofNullable(auditInfo.creator()).ifPresent(builder::setCreator);
    Optional.ofNullable(auditInfo.createTime())
        .map(ProtoUtils::fromInstant)
        .ifPresent(builder::setCreateTime);
    Optional.ofNullable(auditInfo.lastModifier()).ifPresent(builder::setLastModifier);
    Optional.ofNullable(auditInfo.lastModifiedTime())
        .map(ProtoUtils::fromInstant)
        .ifPresent(builder::setLastModifiedTime);

    return builder.build();
  }

  /**
   * Deserializes a {@link AuditInfo} object to an {@link org.apache.gravitino.meta.AuditInfo}
   * object.
   *
   * @param p The serialized AuditInfo object.
   * @return The deserialized AuditInfo object.
   */
  @Override
  public org.apache.gravitino.meta.AuditInfo deserialize(AuditInfo p, Namespace namespace) {
    org.apache.gravitino.meta.AuditInfo.Builder builder =
        org.apache.gravitino.meta.AuditInfo.builder();

    if (p.hasCreator()) builder.withCreator(p.getCreator());
    if (p.hasCreateTime()) builder.withCreateTime(ProtoUtils.toInstant(p.getCreateTime()));
    if (p.hasLastModifier()) builder.withLastModifier(p.getLastModifier());
    if (p.hasLastModifiedTime()) {
      builder.withLastModifiedTime(ProtoUtils.toInstant(p.getLastModifiedTime()));
    }

    return builder.build();
  }
}
