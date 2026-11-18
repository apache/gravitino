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
package org.apache.iceberg;

import com.google.common.base.Preconditions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;

/** Helpers for committing an already-read metadata snapshot to a metastore catalog. */
public class MetastoreRegisterTableUtils {

  /**
   * Registers a metadata snapshot already read and authorized by the caller, without reopening its
   * file. Register overwrite remains unsupported, matching the JDBC backend on this branch.
   *
   * @param catalog destination catalog
   * @param identifier destination table
   * @param metadata snapshot read from the requested metadata file
   * @param overwrite whether replacement was requested; unsupported on this branch
   */
  public static void registerTableWithMetadata(
      BaseMetastoreCatalog catalog,
      TableIdentifier identifier,
      TableMetadata metadata,
      boolean overwrite) {
    if (overwrite) {
      throw new UnsupportedOperationException("Registering tables with overwrite is not supported");
    }
    Preconditions.checkArgument(
        identifier != null && catalog.isValidIdentifier(identifier),
        "Invalid identifier: %s",
        identifier);
    Preconditions.checkArgument(
        metadata.metadataFileLocation() != null,
        "Registered metadata must retain its file location");
    TableOperations ops = catalog.newTableOps(identifier);
    TableMetadata base = ops.current();
    if (base != null) {
      throw new AlreadyExistsException("Table already exists: %s", identifier);
    }
    ops.commit(null, metadata);
  }

  private MetastoreRegisterTableUtils() {}
}
