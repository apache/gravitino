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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;

/**
 * The context passed to a {@link SupportsSchemaLocations} provider on both of its callbacks: when a
 * schema is being created, and when a schema has been dropped.
 *
 * <p>As with {@link TableLocationContext}, one type serves both callbacks so that a new input
 * becomes an additional accessor rather than a signature change. Build instances through {@link
 * #builder()}.
 */
public class SchemaLocationContext {

  private final NameIdentifier schemaIdentifier;

  private final Map<String, String> schemaProperties;

  private final Map<String, String> catalogProperties;

  private final boolean cascade;

  private final List<NameIdentifier> tablesWithRetainedData;

  private SchemaLocationContext(
      NameIdentifier schemaIdentifier,
      Map<String, String> schemaProperties,
      Map<String, String> catalogProperties,
      boolean cascade,
      List<NameIdentifier> tablesWithRetainedData) {
    this.schemaIdentifier = schemaIdentifier;
    this.schemaProperties = schemaProperties;
    this.catalogProperties = catalogProperties;
    this.cascade = cascade;
    this.tablesWithRetainedData = tablesWithRetainedData;
  }

  /**
   * Returns the identifier of the schema this context is about, in the form of {@code
   * metalake.catalog.schema}.
   *
   * @return the schema identifier, never null
   */
  public NameIdentifier schemaIdentifier() {
    return schemaIdentifier;
  }

  /**
   * Returns the properties of the schema this context is about. Which properties those are depends
   * on the callback being served:
   *
   * <ul>
   *   <li>{@link SupportsSchemaLocations#provisionSchemaLocation(SchemaLocationContext)} gets the
   *       properties of the creation request, so the {@code location} entry holds whatever the
   *       caller supplied, which may be nothing at all.
   *   <li>{@link SupportsSchemaLocations#unprovisionSchemaLocation(SchemaLocationContext)} gets the
   *       stored properties of the schema, read right before it was removed, so the {@code
   *       location} entry holds the location that was stored for it.
   * </ul>
   *
   * <p>Values are carried through as they were given, null ones included.
   *
   * @return the schema properties, never null but possibly empty
   */
  public Map<String, String> schemaProperties() {
    return schemaProperties;
  }

  /**
   * Returns the properties of the catalog this provider belongs to, as they were given when the
   * catalog was created. See {@link TableLocationContext#catalogProperties()}.
   *
   * @return the catalog properties, never null but possibly empty
   */
  public Map<String, String> catalogProperties() {
    return catalogProperties;
  }

  /**
   * Returns whether the drop cascaded to the tables of the schema. Always false when the context is
   * passed to {@link SupportsSchemaLocations#provisionSchemaLocation(SchemaLocationContext)}.
   *
   * @return true if the schema was dropped with cascade
   */
  public boolean cascade() {
    return cascade;
  }

  /**
   * Returns the tables the cascade dropped without deleting their data: external tables that were
   * dropped rather than purged, whose datasets the table format left in place and whose locations
   * were not unprovisioned. Whether a table is external is decided the same way the drop path
   * decides it.
   *
   * <p>A non-empty list means the schema location may still hold data. Always empty when the
   * context is passed to {@link
   * SupportsSchemaLocations#provisionSchemaLocation(SchemaLocationContext)} or when the drop did
   * not cascade.
   *
   * @return the identifiers of the tables whose data was retained, never null but possibly empty
   */
  public List<NameIdentifier> tablesWithRetainedData() {
    return tablesWithRetainedData;
  }

  /**
   * Creates a builder for {@link SchemaLocationContext}.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** A builder for {@link SchemaLocationContext}. */
  public static class Builder {

    private NameIdentifier schemaIdentifier;

    private Map<String, String> schemaProperties = ImmutableMap.of();

    private Map<String, String> catalogProperties = ImmutableMap.of();

    private boolean cascade;

    private List<NameIdentifier> tablesWithRetainedData = ImmutableList.of();

    private Builder() {}

    /**
     * Sets the identifier of the schema this context is about.
     *
     * @param schemaIdentifier the schema identifier
     * @return this builder
     */
    public Builder withSchemaIdentifier(NameIdentifier schemaIdentifier) {
      this.schemaIdentifier = schemaIdentifier;
      return this;
    }

    /**
     * Sets the properties of the schema this context is about. A null map is treated as an empty
     * one; a null value under a key is kept as it is.
     *
     * @param schemaProperties the schema properties
     * @return this builder
     */
    public Builder withSchemaProperties(Map<String, String> schemaProperties) {
      this.schemaProperties = copyOf(schemaProperties);
      return this;
    }

    /**
     * Sets the properties of the catalog the schema belongs to. A null map is treated as an empty
     * one; a null value under a key is kept as it is.
     *
     * @param catalogProperties the catalog properties
     * @return this builder
     */
    public Builder withCatalogProperties(Map<String, String> catalogProperties) {
      this.catalogProperties = copyOf(catalogProperties);
      return this;
    }

    /**
     * Sets whether the drop cascaded to the tables of the schema.
     *
     * @param cascade whether the drop cascaded
     * @return this builder
     */
    public Builder withCascade(boolean cascade) {
      this.cascade = cascade;
      return this;
    }

    /**
     * Sets the tables the cascade dropped without deleting their data. A null list is treated as an
     * empty one.
     *
     * @param tablesWithRetainedData the identifiers of those tables
     * @return this builder
     */
    public Builder withTablesWithRetainedData(List<NameIdentifier> tablesWithRetainedData) {
      this.tablesWithRetainedData =
          tablesWithRetainedData == null
              ? ImmutableList.of()
              : ImmutableList.copyOf(tablesWithRetainedData);
      return this;
    }

    /**
     * Builds the {@link SchemaLocationContext}.
     *
     * @return the built context
     */
    public SchemaLocationContext build() {
      Preconditions.checkArgument(schemaIdentifier != null, "schemaIdentifier must not be null");
      return new SchemaLocationContext(
          schemaIdentifier, schemaProperties, catalogProperties, cascade, tablesWithRetainedData);
    }

    // Tolerates null values, which ImmutableMap.copyOf would reject; see
    // TableLocationContext.Builder#withTableProperties.
    private static Map<String, String> copyOf(Map<String, String> properties) {
      return properties == null
          ? ImmutableMap.of()
          : Collections.unmodifiableMap(Maps.newHashMap(properties));
    }
  }
}
