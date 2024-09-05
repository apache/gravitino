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
import org.apache.gravitino.meta.FilesetEntity;

public class FilesetEntitySerDe implements ProtoSerDe<FilesetEntity, Fileset> {
  @Override
  public Fileset serialize(FilesetEntity filesetEntity) {
    Fileset.Builder builder =
        Fileset.newBuilder()
            .setId(filesetEntity.id())
            .setName(filesetEntity.name())
            .setStorageLocation(filesetEntity.storageLocation())
            .setAuditInfo(new AuditInfoSerDe().serialize(filesetEntity.auditInfo()));

    if (filesetEntity.comment() != null) {
      builder.setComment(filesetEntity.comment());
    }

    if (filesetEntity.properties() != null && !filesetEntity.properties().isEmpty()) {
      builder.putAllProperties(filesetEntity.properties());
    }

    Fileset.Type type = Fileset.Type.valueOf(filesetEntity.filesetType().name());
    builder.setType(type);

    return builder.build();
  }

  @Override
  public FilesetEntity deserialize(Fileset p, Namespace namespace) {
    FilesetEntity.Builder builder =
        FilesetEntity.builder()
            .withId(p.getId())
            .withName(p.getName())
            .withNamespace(namespace)
            .withStorageLocation(p.getStorageLocation())
            .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo(), namespace))
            .withFilesetType(org.apache.gravitino.file.Fileset.Type.valueOf(p.getType().name()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    return builder.build();
  }
}
