/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.iceberg.inmemory;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;

/** In-memory metastore pointer swap used by {@link org.apache.iceberg.RegisterTableOverwrite}. */
public final class InMemoryRegisterTablePointerSwap {

  private InMemoryRegisterTablePointerSwap() {}

  @SuppressWarnings("unchecked")
  public static void swap(
      InMemoryCatalog catalog,
      TableIdentifier tableIdentifier,
      String oldMetadataLocation,
      String newMetadataLocation) {
    try {
      ConcurrentMap<TableIdentifier, String> tables =
          (ConcurrentMap<TableIdentifier, String>) FieldUtils.readField(catalog, "tables", true);

      tables.compute(
          tableIdentifier,
          (ignored, existingLocation) -> {
            if (!Objects.equals(existingLocation, oldMetadataLocation)) {
              if (existingLocation == null) {
                throw new CommitFailedException("Table does not exist: %s", tableIdentifier);
              }

              throw new CommitFailedException(
                  "Cannot overwrite table %s metadata location from %s to %s because it has been "
                      + "concurrently modified to %s",
                  tableIdentifier, oldMetadataLocation, newMetadataLocation, existingLocation);
            }
            return newMetadataLocation;
          });
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to read in-memory catalog tables field", e);
    }
  }
}
