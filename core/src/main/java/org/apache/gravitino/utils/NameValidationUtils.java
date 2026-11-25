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

import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObjects;

/** Shared validation for Gravitino object names. */
public final class NameValidationUtils {
  /** Reserved name of the system catalog. */
  public static final String SYSTEM_CATALOG_RESERVED_NAME = "system";

  private static final int DEFAULT_MAX_NAME_LENGTH = 64;
  private static final Pattern NAME_PATTERN = Pattern.compile("\\w[\\w-]*");

  private NameValidationUtils() {}

  /**
   * Validates a catalog or domain name.
   *
   * @param type The entity type used in validation messages.
   * @param name The proposed name.
   * @throws IllegalArgumentException If the name is reserved or illegal.
   */
  public static void validateName(String type, String name) {
    Preconditions.checkArgument(name != null, "The %s name must not be null.", type);
    if (SYSTEM_CATALOG_RESERVED_NAME.equalsIgnoreCase(name)
        || MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(name)) {
      throw new IllegalArgumentException("The " + type + " name '" + name + "' is reserved.");
    }
    String error = nameValidationError(type, name, NAME_PATTERN);
    if (error != null) {
      throw new IllegalArgumentException(error);
    }
  }

  /**
   * Checks a name against its syntax and the default Gravitino name length limit.
   *
   * @param type The object type used in the error message.
   * @param name The proposed name.
   * @param syntax The pattern for a valid name, excluding the length limit.
   * @return The reason for rejection, or null if the name is valid.
   */
  @Nullable
  public static String nameValidationError(String type, String name, Pattern syntax) {
    if (!syntax.matcher(name).matches()) {
      return String.format(Locale.ROOT, "The %s name '%s' is illegal.", type, name);
    }
    if (name.codePointCount(0, name.length()) > DEFAULT_MAX_NAME_LENGTH) {
      return String.format(
          Locale.ROOT, "The %s name must not exceed %d characters.", type, DEFAULT_MAX_NAME_LENGTH);
    }
    return null;
  }

  /**
   * Validates the domain names in a member filter.
   *
   * @param names The requested domain names.
   * @throws IllegalArgumentException If the collection is null or any name is reserved or illegal.
   */
  public static void validateDomainNames(Iterable<String> names) {
    Preconditions.checkArgument(names != null, "Domain names must not be null.");
    names.forEach(name -> validateName("domain", name));
  }
}
