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
package org.apache.gravitino.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;

public class MetadataObjectUtil {

  private static final Joiner DOT = Joiner.on(".");

  private static final BiMap<MetadataObject.Type, Entity.EntityType> TYPE_TO_TYPE_MAP =
      ImmutableBiMap.<MetadataObject.Type, Entity.EntityType>builder()
          .put(MetadataObject.Type.METALAKE, Entity.EntityType.METALAKE)
          .put(MetadataObject.Type.CATALOG, Entity.EntityType.CATALOG)
          .put(MetadataObject.Type.SCHEMA, Entity.EntityType.SCHEMA)
          .put(MetadataObject.Type.TABLE, Entity.EntityType.TABLE)
          .put(MetadataObject.Type.TOPIC, Entity.EntityType.TOPIC)
          .put(MetadataObject.Type.FILESET, Entity.EntityType.FILESET)
          .put(MetadataObject.Type.COLUMN, Entity.EntityType.COLUMN)
          .put(MetadataObject.Type.ROLE, Entity.EntityType.ROLE)
          .build();

  private MetadataObjectUtil() {}

  /**
   * Map the given {@link MetadataObject}'s type to the corresponding {@link Entity.EntityType}.
   *
   * @param metadataObject The metadata object
   * @return The entity type
   * @throws IllegalArgumentException if the metadata object type is unknown
   */
  public static Entity.EntityType toEntityType(MetadataObject metadataObject) {
    Preconditions.checkArgument(metadataObject != null, "metadataObject cannot be null");

    return Optional.ofNullable(TYPE_TO_TYPE_MAP.get(metadataObject.type()))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Unknown metadata object type: " + metadataObject.type()));
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
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName), "metalakeName cannot be blank");
    Preconditions.checkArgument(metadataObject != null, "metadataObject cannot be null");

    switch (metadataObject.type()) {
      case METALAKE:
        return NameIdentifierUtil.ofMetalake(metalakeName);
      case ROLE:
        return AuthorizationUtils.ofRole(metalakeName, metadataObject.name());
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
