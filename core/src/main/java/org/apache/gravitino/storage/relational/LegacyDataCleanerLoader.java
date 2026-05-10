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
package org.apache.gravitino.storage.relational;

import java.io.IOException;
import java.util.ServiceLoader;
import org.apache.gravitino.Entity;

/** Loads plugin-provided legacy data cleaners. */
public final class LegacyDataCleanerLoader {

  private LegacyDataCleanerLoader() {}

  public static LegacyDataCleaner load(Entity.EntityType entityType) throws IOException {
    return load(entityType, ServiceLoader.load(LegacyDataCleaner.class));
  }

  public static LegacyDataCleaner load(
      Entity.EntityType entityType, Iterable<LegacyDataCleaner> cleaners) throws IOException {
    LegacyDataCleaner matchedCleaner = null;
    for (LegacyDataCleaner cleaner : cleaners) {
      if (cleaner.entityType() != entityType) {
        continue;
      }

      if (matchedCleaner != null) {
        throw new IOException(
            String.format("Multiple LegacyDataCleaner implementations found for %s", entityType));
      }

      matchedCleaner = cleaner;
    }

    if (matchedCleaner == null) {
      throw new IOException(
          String.format("No LegacyDataCleaner implementation found for %s", entityType));
    }

    return matchedCleaner;
  }
}
