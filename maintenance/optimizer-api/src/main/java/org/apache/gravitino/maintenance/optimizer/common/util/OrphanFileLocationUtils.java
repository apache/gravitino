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

import java.net.URI;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;

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
    IcebergOrphanFileRemovalContent.validateLocationWithinTable(tableLocation, location);
  }

  /**
   * Normalizes an absolute location and rejects ambiguous URI encodings.
   *
   * @param value input location
   * @return normalized URI
   */
  public static URI normalizeLocation(String value) {
    return IcebergOrphanFileRemovalContent.normalizeLocation(value);
  }
}
