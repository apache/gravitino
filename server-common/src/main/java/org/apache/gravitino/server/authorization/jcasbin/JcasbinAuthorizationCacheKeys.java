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

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.MetadataObject;

/** Cache key factory for JCasbin authorization caches. */
final class JcasbinAuthorizationCacheKeys {

  /** Unit Separator for internal cache keys. */
  static final String SEPARATOR = "\u001F";

  private JcasbinAuthorizationCacheKeys() {}

  /**
   * Builds a path-based key for the metadata id cache.
   *
   * <p>Container objects end with the internal separator so prefix invalidation can remove both the
   * container and entries under the same name path. Leaf objects include the type suffix to avoid
   * collisions between objects that share the same name path.
   */
  static String metadataObjectKey(String metalake, MetadataObject metadataObject) {
    if (metadataObject.type() == MetadataObject.Type.METALAKE) {
      return metalake + SEPARATOR;
    }

    StringBuilder sb = new StringBuilder(metalake);
    sb.append(SEPARATOR);
    sb.append(String.join(SEPARATOR, metadataObject.fullName().split("\\.")));
    if (isMetadataContainer(metadataObject.type())) {
      sb.append(SEPARATOR);
    } else {
      sb.append(SEPARATOR);
      sb.append(metadataObject.type().name());
    }
    return sb.toString();
  }

  static String userRoleKey(String metalake, String username) {
    return "USER" + SEPARATOR + metalake + SEPARATOR + username;
  }

  static String groupRoleKey(String metalake, String groupname) {
    return "GROUP" + SEPARATOR + metalake + SEPARATOR + groupname;
  }

  @VisibleForTesting
  static boolean isMetadataContainer(MetadataObject.Type type) {
    return type == MetadataObject.Type.METALAKE
        || type == MetadataObject.Type.CATALOG
        || type == MetadataObject.Type.SCHEMA;
  }
}
