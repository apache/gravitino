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

import static org.apache.gravitino.catalog.glue.GlueConstants.METADATA_LOCATION;
import static org.apache.gravitino.catalog.glue.GlueConstants.TABLE_FORMAT_TYPE;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Properties metadata for Glue tables.
 *
 * <p>Defines well-known Glue {@code Table.parameters()} keys that Gravitino exposes. All entries
 * are optional and mutable, reflecting that Glue stores them as free-form key-value pairs. Unknown
 * parameters from {@code Table.parameters()} are passed through transparently by the catalog
 * operations layer and are not validated here.
 *
 * <p>Note: storage location ({@code StorageDescriptor.location}) varies by table format and is
 * handled per-format in the Table CRUD layer, not declared here.
 */
public class GlueTablePropertiesMetadata extends BasePropertiesMetadata {

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              TABLE_FORMAT_TYPE,
              stringOptionalPropertyEntry(
                  TABLE_FORMAT_TYPE,
                  "Glue table format type stored in Table.parameters(). Common values:"
                      + " ICEBERG, HIVE, DELTA, PARQUET.",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .put(
              METADATA_LOCATION,
              stringOptionalPropertyEntry(
                  METADATA_LOCATION,
                  "Iceberg metadata file location stored in Table.parameters().",
                  false /* immutable */,
                  null /* defaultValue */,
                  false /* hidden */))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
