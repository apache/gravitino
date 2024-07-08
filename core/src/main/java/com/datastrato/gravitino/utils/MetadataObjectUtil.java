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
package com.datastrato.gravitino.utils;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.NameIdentifier;
import com.google.common.base.Joiner;

public class MetadataObjectUtil {

  private static final Joiner DOT = Joiner.on(".");

  private MetadataObjectUtil() {}

  /**
   * Map the given {@link MetadataObject}'s type to the corresponding {@link Entity.EntityType}.
   *
   * @param metadataObject The metadata object
   * @return The entity type
   * @throws IllegalArgumentException if the metadata object type is unknown
   */
  public static Entity.EntityType toEntityType(MetadataObject metadataObject) {
    switch (metadataObject.type()) {
      case METALAKE:
        return Entity.EntityType.METALAKE;
      case CATALOG:
        return Entity.EntityType.CATALOG;
      case SCHEMA:
        return Entity.EntityType.SCHEMA;
      case TABLE:
        return Entity.EntityType.TABLE;
      case TOPIC:
        return Entity.EntityType.TOPIC;
      case FILESET:
        return Entity.EntityType.FILESET;
      case COLUMN:
        return Entity.EntityType.COLUMN;
      default:
        throw new IllegalArgumentException(
            "Unknown metadata object type: " + metadataObject.type());
    }
  }

  /**
   * Convert the given {@link MetadataObject} full name to the corresponding {@link NameIdentifier}.
   *
   * @param metalakeName The metalake name
   * @param metadataObject The metadata object
   * @return The entity identifier
   * @throws IllegalArgumentException if the metadata object type is unsupported or unknown.
   */
  public static NameIdentifier toEntityIdent(String metalakeName, MetadataObject metadataObject) {
    switch (metadataObject.type()) {
      case METALAKE:
        return NameIdentifierUtil.ofMetalake(metalakeName);
      case CATALOG:
      case SCHEMA:
      case TABLE:
      case TOPIC:
      case FILESET:
        String fullName = DOT.join(metalakeName, metadataObject.fullName());
        return NameIdentifier.parse(fullName);
      case COLUMN:
        throw new IllegalArgumentException(
            "Cannot convert column metadata object to entity identifier: "
                + metadataObject.fullName());
      default:
        throw new IllegalArgumentException(
            "Unknown metadata object type: " + metadataObject.type());
    }
  }
}
