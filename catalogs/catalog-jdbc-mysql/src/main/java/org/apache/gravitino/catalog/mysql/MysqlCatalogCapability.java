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
package org.apache.gravitino.catalog.mysql;

import java.util.Set;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

public class MysqlCatalogCapability implements Capability {
  /**
   * Regular expression explanation: ^[\w\p{L}-$/=]{1,64}$
   *
   * <p>^ - Start of the string
   *
   * <p>[\w\p{L}-$/=]{1,64} - Consist of 1 to 64 characters of letters (both cases), digits,
   * underscores, any kind of letter from any language, hyphens, dollar signs, slashes or equal
   * signs
   *
   * <p>\w - matches [a-zA-Z0-9_]
   *
   * <p>\p{L} - matches any kind of letter from any language
   *
   * <p>$ - End of the string
   */
  public static final String MYSQL_NAME_PATTERN = "^[\\w\\p{L}-$/=]{1,64}$";

  /** Reserved schema andtable names in MySQL that cannot be used for user-defined schemas. */
  private static final Set<String> MYSQL_RESERVED_SCHEMAS =
      Set.of("mysql", "information_schema", "performance_schema", "sys");

  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    if (!name.matches(MYSQL_NAME_PATTERN)) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal.", scope, name));
    }

    if (scope == Scope.SCHEMA && MYSQL_RESERVED_SCHEMAS.contains(name.toLowerCase())) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is reserved and cannot be used.", scope, name));
    }

    return CapabilityResult.SUPPORTED;
  }
}
