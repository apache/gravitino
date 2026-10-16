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
package org.apache.gravitino.maintenance.optimizer.common.util;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Objects;

/** Lexical containment checks shared by orphan cleanup submission and execution. */
public final class OrphanFileLocationUtils {
  private OrphanFileLocationUtils() {}

  /**
   * Validates that a scan is within the table root. Filesystem symlink validation must also run in
   * the job, using its storage credentials and filesystem view.
   *
   * @param tableLocation table storage root
   * @param location requested scan location
   */
  public static void validateLocation(String tableLocation, String location) {
    URI root = normalizeLocation(tableLocation);
    URI requested = normalizeLocation(location);
    String rootPath = root.getPath().replaceAll("/+$", "");
    String childPath = requested.getPath().replaceAll("/+$", "");
    boolean sameStorage =
        Objects.equals(root.getScheme(), requested.getScheme())
            && Objects.equals(root.getAuthority(), requested.getAuthority());
    Preconditions.checkArgument(
        sameStorage
            && (childPath.equals(rootPath)
                || childPath.startsWith(rootPath.endsWith("/") ? rootPath : rootPath + "/")),
        "location must be within the table's storage location: %s",
        tableLocation);
  }

  /**
   * Normalizes an absolute location and rejects ambiguous URI encodings.
   *
   * @param value input location
   * @return normalized URI
   */
  public static URI normalizeLocation(String value) {
    // Reject ambiguous encoded paths rather than allowing different filesystem decoders to
    // interpret the containment check and the subsequent listing differently.
    Preconditions.checkArgument(
        !value.isEmpty() && !value.contains("%") && !value.contains("\\"),
        "Invalid scan location: %s",
        value);
    URI uri = URI.create(value);
    Preconditions.checkArgument(
        uri.getQuery() == null
            && uri.getFragment() == null
            && uri.getPath() != null
            && uri.getPath().startsWith("/"),
        "Scan location must be an absolute path without query or fragment: %s",
        value);
    if (uri.getScheme() == null || "file".equals(uri.getScheme())) {
      return (uri.getScheme() == null ? Paths.get(value) : Paths.get(uri)).normalize().toUri();
    }
    return uri.normalize();
  }
}
