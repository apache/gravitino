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

import java.util.regex.Pattern;
import org.apache.gravitino.MetadataObjects;

/** Naming rules shared by catalogs and domains. */
public final class NameValidationUtils {
  /** Reserved name of the system catalog. */
  public static final String SYSTEM_CATALOG_RESERVED_NAME = "system";

  private static final Pattern NAME_PATTERN = Pattern.compile("^\\w[\\w-]{0,63}$");

  private NameValidationUtils() {}

  /**
   * Validates a catalog or domain name.
   *
   * @param type The entity type used in validation messages.
   * @param name The proposed name.
   * @throws IllegalArgumentException If the name is reserved or illegal.
   */
  public static void validateName(String type, String name) {
    if (SYSTEM_CATALOG_RESERVED_NAME.equalsIgnoreCase(name)
        || MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(name)) {
      throw new IllegalArgumentException("The " + type + " name '" + name + "' is reserved.");
    }
    if (name == null || !NAME_PATTERN.matcher(name).matches()) {
      throw new IllegalArgumentException("The " + type + " name '" + name + "' is illegal.");
    }
  }

  /**
   * Validates the domain names in a member filter.
   *
   * @param names The requested domain names.
   * @throws IllegalArgumentException If any name is reserved or illegal.
   */
  public static void validateDomainNames(Iterable<String> names) {
    names.forEach(name -> validateName("domain", name));
  }
}
