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

package org.apache.gravitino.iceberg.common.cache;

import org.apache.iceberg.catalog.TableIdentifier;

/** An interface that supports retrieving the metadata location for a table. */
public interface SupportsMetadataLocation {
  /** The property key used to store or reference the metadata location of a table. */
  String METADATA_LOCATION_PROP = "metadata_location";

  /**
   * Retrieves the metadata location for the specified table.
   *
   * @param tableIdentifier the identifier of the table to retrieve metadata location for
   * @return the metadata location of the table as a String
   */
  String metadataLocation(TableIdentifier tableIdentifier);
}
