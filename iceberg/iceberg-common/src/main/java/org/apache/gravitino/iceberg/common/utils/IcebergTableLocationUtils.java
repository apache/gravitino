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
package org.apache.gravitino.iceberg.common.utils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/** Location comparisons and JDBC lookup patterns for Iceberg's standard metadata layout. */
public final class IcebergTableLocationUtils {
  private static final String METADATA_DIRECTORY = "/metadata/";

  private IcebergTableLocationUtils() {}

  /**
   * Normalizes trailing separators, local URI forms and equivalent S3 schemes.
   *
   * @param location storage location, possibly absent
   * @return normalized location, or an empty string for an absent location
   */
  public static String normalize(@Nullable String location) {
    if (location == null) {
      return "";
    }
    String value = location.trim();
    if (value.startsWith("s3a://") || value.startsWith("s3n://")) {
      value = "s3://" + value.substring(6);
    } else if (value.startsWith("file:")) {
      String path = value.substring(5);
      while (path.startsWith("//")) {
        path = path.substring(1);
      }
      value = "file:" + (path.startsWith("/") ? path : "/" + path);
    } else if (value.startsWith("/")) {
      value = "file:" + value;
    }
    while (value.endsWith("/") && !value.endsWith(":/")) {
      value = value.substring(0, value.length() - 1);
    }
    return value;
  }

  /**
   * Tests equality or containment at a whole path-segment boundary.
   *
   * @param left first storage location
   * @param right second storage location
   * @return whether either location contains the other
   */
  public static boolean overlaps(String left, String right) {
    String a = normalize(left);
    String b = normalize(right);
    return !a.isEmpty()
        && !b.isEmpty()
        && (a.equals(b) || a.startsWith(directory(b)) || b.startsWith(directory(a)));
  }

  /**
   * Infers the table root for a standard {@code <table>/metadata/<file>} pointer.
   *
   * @param metadataLocation current metadata pointer
   * @return the inferred root, or empty for a nonstandard layout
   */
  public static Optional<String> tableLocation(String metadataLocation) {
    String value = normalize(metadataLocation);
    int index = value.lastIndexOf(METADATA_DIRECTORY);
    if (index < 0 || value.substring(index + METADATA_DIRECTORY.length()).contains("/")) {
      return Optional.empty();
    }
    return Optional.of(value.substring(0, index));
  }

  /**
   * Builds bound LIKE patterns for overlapping standard metadata pointers. The SQL must use {@code
   * ESCAPE '!'}; user-supplied percent signs, underscores and escape characters are literal.
   *
   * @param location requested table location
   * @return patterns covering descendants and the metadata directories of ancestors
   */
  public static List<String> metadataLocationPatterns(String location) {
    String normalized = normalize(location);
    Set<String> forms = new LinkedHashSet<>();
    forms.add(normalized);
    if (normalized.startsWith("s3://")) {
      forms.add("s3a://" + normalized.substring(5));
      forms.add("s3n://" + normalized.substring(5));
    } else if (normalized.startsWith("file:/")) {
      forms.add("file://" + normalized.substring(5));
      forms.add(normalized.substring(5));
    }
    Set<String> patterns = new LinkedHashSet<>();
    for (String form : forms) {
      patterns.add(escape(directory(form)) + "%");
      String parent = form;
      while (!parent.isEmpty()) {
        patterns.add(escape(directory(parent) + "metadata/") + "%");
        int slash = parent.lastIndexOf('/');
        int scheme = parent.indexOf("://");
        if (slash <= (scheme >= 0 ? scheme + 2 : 0)) {
          break;
        }
        parent = parent.substring(0, slash);
      }
    }
    return new ArrayList<>(patterns);
  }

  private static String directory(String value) {
    return value.endsWith("/") ? value : value + "/";
  }

  private static String escape(String value) {
    return value.replace("!", "!!").replace("%", "!%").replace("_", "!_");
  }
}
