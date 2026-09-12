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
 *   <li>the table's own {@code location} property, with a trailing slash added;
 *   <li>the schema's {@code location} property, with the table name appended;
 *   <li>the catalog's {@code location} property, with the schema and table names appended.
 * </ol>
 *
 * If none of them is set, provisioning fails with an {@link IllegalArgumentException}.
 *
 * <p>The first level is unreachable through the catalog, which keeps a caller-supplied location
 * without consulting any provider. It is kept so that the three levels still read as one rule, and
 * so that calling this provider directly behaves as it always has.
 */
public class DefaultTableLocationProvider implements TableLocationProvider {

  /** The name under which this provider is registered. */
  public static final String NAME = "default";

  private static final String SLASH = "/";

  private volatile Optional<String> catalogLocation = Optional.empty();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    String location = catalogProperties.get(Catalog.PROPERTY_LOCATION);
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
        + schema.name()
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

  /**
   * Does nothing, for the same reason as {@link #unprovisionTableLocation(TableLocationContext)}: a
   * composed location was never allocated anywhere, so a location the table did not use costs
   * nothing and there is nothing to release. Written out rather than inherited so that the two
   * callbacks are visibly deliberate here, not overlooked.
   *
   * @param context the table that was created, unused
   */
  @Override
  public void releaseUnusedLocation(TableLocationContext context) {}

  /**
   * Appends a trailing slash to the given path unless it already ends with one.
   *
   * <p>Package-private rather than private so that {@link GenericCatalogOperations} can apply the
   * very same normalization to a caller-supplied location, which bypasses this provider, and when
   * comparing a provisioned location against the one the created table reports. Sharing the method
   * is what keeps those and this provider from drifting apart.
   *
   * @param path the path to normalize
   * @return the path, ending with a slash
   */
  static String ensureTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path : path + SLASH;
  }
}
