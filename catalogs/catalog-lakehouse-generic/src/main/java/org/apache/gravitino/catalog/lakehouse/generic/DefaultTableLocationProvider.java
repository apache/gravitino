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
package org.apache.gravitino.catalog.lakehouse.generic;

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.rel.Table;

/**
 * The built-in {@link TableLocationProvider}, used when the {@code table-location-provider} catalog
 * property is not set.
 *
 * <p>It derives the table location from the {@code location} property, falling back through three
 * levels:
 *
 * <ol>
 *   <li>the table's own {@code location} property, used as-is;
 *   <li>the schema's {@code location} property, with the table name appended;
 *   <li>the catalog's {@code location} property, with the schema and table names appended.
 * </ol>
 *
 * If none of them is set, provisioning fails with an {@link IllegalArgumentException}.
 */
public class DefaultTableLocationProvider implements TableLocationProvider {

  /** The name under which this provider is registered. */
  public static final String NAME = "default";

  private static final String SLASH = "/";

  private Optional<String> catalogLocation = Optional.empty();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    String location =
        catalogProperties == null ? null : catalogProperties.get(Catalog.PROPERTY_LOCATION);
    this.catalogLocation =
        StringUtils.isNotBlank(location)
            ? Optional.of(location).map(DefaultTableLocationProvider::ensureTrailingSlash)
            : Optional.empty();
  }

  @Override
  public String provisionTableLocation(TableLocationContext context) {
    NameIdentifier tableIdent = context.tableIdentifier();
    Schema schema = context.schema();

    String tableLocation = context.tableProperties().get(Table.PROPERTY_LOCATION);
    if (StringUtils.isNotBlank(tableLocation)) {
      return ensureTrailingSlash(tableLocation);
    }

    String schemaLocation =
        schema.properties() == null ? null : schema.properties().get(Schema.PROPERTY_LOCATION);

    // If we do not set location in table properties, and schema location is set, use schema
    // location as the base path.
    if (StringUtils.isNotBlank(schemaLocation)) {
      return ensureTrailingSlash(schemaLocation) + tableIdent.name() + SLASH;
    }

    // If the schema location is not set, use catalog lakehouse dir as the base path. Or else, throw
    // an exception.
    if (catalogLocation.isEmpty()) {
      throw new IllegalArgumentException(
          "'location' property is neither set in table properties "
              + "nor in schema properties, and no location is set in catalog properties either. "
              + "Please set the 'location' in either of them to create the table "
              + tableIdent);
    }

    return ensureTrailingSlash(catalogLocation.get())
        + tableIdent.namespace().level(2)
        + SLASH
        + tableIdent.name()
        + SLASH;
  }

  /**
   * Does nothing: this provider composes the location from the table, schema and catalog {@code
   * location} properties and registers it nowhere, so there is nothing to hand back. Deleting the
   * data itself stays the responsibility of the table format.
   *
   * @param context the table that was dropped, unused
   */
  @Override
  public void unprovisionTableLocation(TableLocationContext context) {}

  private static String ensureTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path : path + SLASH;
  }
}
