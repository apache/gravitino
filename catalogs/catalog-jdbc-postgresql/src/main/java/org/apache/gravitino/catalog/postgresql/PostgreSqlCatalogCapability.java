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
package org.apache.gravitino.catalog.postgresql;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

public class PostgreSqlCatalogCapability implements Capability {
  /**
   * Regular expression explanation: ^[_a-zA-Z\p{L}/][\w\p{L}-$/=]{0,62}$
   *
   * <p>^[_a-zA-Z\p{L}/] - Start with an underscore, a letter, or a letter from any language
   *
   * <p>[\w\p{L}-$/=]{0,62} - Consist of 0 to 62 characters (making the total length at most 63) of
   * letters (both cases), digits, underscores, any kind of letter from any language, hyphens,
   * dollar signs, slashes or equal signs
   *
   * <p>$ - End of the string
   */
  public static final String POSTGRESQL_NAME_PATTERN = "^[_a-zA-Z\\p{L}/][\\w\\p{L}-$/=]{0,62}$";

  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    // TODO: Validate the name against reserved words
    if (!name.matches(POSTGRESQL_NAME_PATTERN)) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal.", scope, name));
    }
    return CapabilityResult.SUPPORTED;
  }
}
