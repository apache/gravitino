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
package org.apache.gravitino.catalog.jdbc;

import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

public class JdbcCatalogCapability implements Capability {
  /**
   * Regular expression explanation: Regex that matches any string that maybe a filename with an
   * optional extension We adopt a blacklist approach that excludes filename or extension that
   * contains '.', '/', or '\' ^[^.\/\\]+(\.[^.\/\\]+)?$
   *
   * <p>^ - Start of the string
   *
   * <p>[^.\/\\]+ - matches any filename string that does not contain '.', '/', or '\'
   *
   * <p>(\.[^.\/\\]+)? - matches an optional extension
   *
   * <p>$ - End of the string
   */
  // We use sqlite name pattern to be the default pattern for JDBC catalog for testing purposes
  public static final String SQLITE_NAME_PATTERN = "^[^.\\/\\\\]+(\\.[^.\\/\\\\]+)?$";

  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    // TODO: Validate the name against reserved words
    if (!name.matches(SQLITE_NAME_PATTERN)) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal.", scope, name));
    }
    return CapabilityResult.SUPPORTED;
  }
}
