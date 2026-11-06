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
package org.apache.gravitino.catalog.glue;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

/** A {@link GlueCatalog} that can open a table from a metadata location already read from Glue. */
class GravitinoGlueCatalog extends GlueCatalog {

  /**
   * Opens a read-only table at the given metadata location. Unlike {@link #loadTable}, this neither
   * queries Glue nor goes through the metadata refresh, which retries most read failures for about
   * 90 seconds. The metadata file is read on the first access to the returned table.
   *
   * @param identifier the table identifier
   * @param metadataLocation the metadata file location recorded in Glue
   * @return a read-only table that cannot commit changes
   * @throws NoSuchTableException if {@code metadataLocation} is null
   */
  Table loadStaticTable(TableIdentifier identifier, String metadataLocation) {
    if (metadataLocation == null) {
      throw new NoSuchTableException("Table %s has no metadata location", identifier);
    }

    FileIO io = newTableOps(identifier).io();
    return new BaseTable(new StaticTableOperations(metadataLocation, io), identifier.toString());
  }
}
