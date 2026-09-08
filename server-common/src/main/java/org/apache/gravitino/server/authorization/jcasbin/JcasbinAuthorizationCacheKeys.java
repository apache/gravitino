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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.MetadataObject;

/** Cache key factory for JCasbin authorization caches. */
final class JcasbinAuthorizationCacheKeys {

  private static final String KEY_SEPARATOR = "\u001F";

  static final String USER_ROLE_REL = "USER_ROLE_REL";
  static final String GROUP_ROLE_REL = "GROUP_ROLE_REL";

  private JcasbinAuthorizationCacheKeys() {}

  static String metadataIdCacheKey(String metalake, MetadataObject metadataObject) {
    List<String> parts = new ArrayList<>();
    parts.add(metalake);

    String[] names = metadataObject.fullName().split("\\.");
    switch (metadataObject.type()) {
      case METALAKE:
        parts.add(MetadataObject.Type.METALAKE.name());
        break;
      case CATALOG:
        parts.add(MetadataObject.Type.CATALOG.name());
        parts.add(names[0]);
        break;
      case SCHEMA:
        appendCatalogAndSchemas(parts, names);
        break;
      case TABLE:
        appendCatalogSchemasAndLeaf(parts, names, MetadataObject.Type.TABLE);
        break;
      case COLUMN:
        appendCatalogAndSchemas(parts, names, names.length - 2);
        parts.add(MetadataObject.Type.TABLE.name());
        parts.add(names[names.length - 2]);
        parts.add(MetadataObject.Type.COLUMN.name());
        parts.add(names[names.length - 1]);
        break;
      case VIEW:
      case TOPIC:
      case FILESET:
      case MODEL:
      case FUNCTION:
        appendCatalogSchemasAndLeaf(parts, names, metadataObject.type());
        break;
      default:
        parts.add(metadataObject.type().name());
        parts.add(metadataObject.fullName());
        break;
    }

    String key = String.join(KEY_SEPARATOR, parts);
    if (hasNestedMetadataObjects(metadataObject.type())) {
      key += KEY_SEPARATOR;
    }
    return key;
  }

  static String userRoleKey(String metalake, String username) {
    return String.join(KEY_SEPARATOR, metalake, USER_ROLE_REL, username);
  }

  static String groupRoleKey(String metalake, String groupname) {
    return String.join(KEY_SEPARATOR, metalake, GROUP_ROLE_REL, groupname);
  }

  static boolean hasNestedMetadataObjects(MetadataObject.Type type) {
    return type == MetadataObject.Type.METALAKE
        || type == MetadataObject.Type.CATALOG
        || type == MetadataObject.Type.SCHEMA
        || type == MetadataObject.Type.TABLE;
  }

  static String joinKeyParts(String... parts) {
    return String.join(KEY_SEPARATOR, parts);
  }

  private static void appendCatalogAndSchemas(List<String> parts, String[] names) {
    parts.add(MetadataObject.Type.CATALOG.name());
    parts.add(names[0]);
    for (int i = 1; i < names.length; i++) {
      parts.add(MetadataObject.Type.SCHEMA.name());
      parts.add(names[i]);
    }
  }

  private static void appendCatalogSchemasAndLeaf(
      List<String> parts, String[] names, MetadataObject.Type leafType) {
    appendCatalogAndSchemas(parts, names, names.length - 1);
    parts.add(leafType.name());
    parts.add(names[names.length - 1]);
  }

  private static void appendCatalogAndSchemas(
      List<String> parts, String[] names, int exclusiveEnd) {
    parts.add(MetadataObject.Type.CATALOG.name());
    parts.add(names[0]);
    for (int i = 1; i < exclusiveEnd; i++) {
      parts.add(MetadataObject.Type.SCHEMA.name());
      parts.add(names[i]);
    }
  }
}
