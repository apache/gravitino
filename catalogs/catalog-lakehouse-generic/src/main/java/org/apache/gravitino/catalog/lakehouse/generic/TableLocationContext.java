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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.rel.Table;

/**
 * The context passed to a {@link TableLocationProvider} on each of its three callbacks: when a
 * table is being created and its location has to be provisioned, when a location that was
 * provisioned turns out not to be the one the table ended up at and has to be released, and when a
 * table has been dropped and its location has to be unprovisioned.
 *
 * <p>The same type is used for all three so that a new input becomes an additional accessor rather
 * than a signature change: providers written against an older version keep compiling and running.
 * For that reason implementations should not rely on the constructor signature and should build
 * instances through {@link #builder()}.
 */
public class TableLocationContext {

  private final NameIdentifier tableIdentifier;

  private final Map<String, String> tableProperties;

  private final Schema schema;

  private TableLocationContext(
      NameIdentifier tableIdentifier, Map<String, String> tableProperties, Schema schema) {
    this.tableIdentifier = tableIdentifier;
    this.tableProperties = tableProperties;
    this.schema = schema;
  }

  /**
   * Returns the identifier of the table this context is about, in the form of {@code
   * metalake.catalog.schema.table}.
   *
   * @return the table identifier, never null
   */
  public NameIdentifier tableIdentifier() {
    return tableIdentifier;
  }

  /**
   * Returns the properties of the table this context is about. Which properties those are, and in
   * particular whether the {@code location} entry ({@link
   * org.apache.gravitino.rel.Table#PROPERTY_LOCATION}) is filled in, depends on the callback being
   * served:
   *
   * <ul>
   *   <li>{@link TableLocationProvider#provisionTableLocation(TableLocationContext)} gets the
   *       properties of the creation request. There is no location worth reading here: the provider
   *       is consulted only when the request did not carry a non-blank one, so the entry is either
   *       absent or blank.
   *   <li>{@link TableLocationProvider#releaseUnusedLocation(TableLocationContext)} gets the
   *       properties the table was created with, so the location entry is filled in, and it holds
   *       exactly the provisioned location that is to be released.
   *   <li>{@link TableLocationProvider#unprovisionTableLocation(TableLocationContext)} gets the
   *       stored properties of the table being dropped, read right before it was removed, so the
   *       location entry holds the location that was provisioned for it.
   * </ul>
   *
   * <p>Values are carried through as they were given, null ones included: the catalog accepts a
   * property whose value is null, and this context is not the layer that starts to reject it. Read
   * a property with {@code get} rather than assuming that a key which is present has a value.
   *
   * @return the table properties, never null but possibly empty
   */
  public Map<String, String> tableProperties() {
    return tableProperties;
  }

  /**
   * Returns the schema the table belongs to, including its properties.
   *
   * @return the parent schema, never null
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Returns whether the table this context is about is an external table, that is, one whose data
   * the catalog does not own and only points at.
   *
   * <p>This is the {@code external} entry of {@link #tableProperties()}, read with {@link
   * Boolean#parseBoolean(String)}, so only {@code "true"} ignoring case is true and anything else,
   * absent or null included, is false. That is deliberately the same reading the table formats and
   * the property metadata use: a provider that saw {@code external} differently from the format
   * that owns the data would skip reclaiming a location the format had just deleted the data under,
   * or reclaim one it had left alone. It is offered as an accessor because it changes what a
   * provider should do: the catalog does not ask a provider to unprovision the location of an
   * external table, and a provider asked to provision one is being asked for a path for data that
   * may already exist elsewhere.
   *
   * @return true if the table is marked external
   */
  public boolean isExternal() {
    return Boolean.parseBoolean(tableProperties.get(Table.PROPERTY_EXTERNAL));
  }

  /**
   * Creates a builder for {@link TableLocationContext}.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** A builder for {@link TableLocationContext}. */
  public static class Builder {

    private NameIdentifier tableIdentifier;

    private Map<String, String> tableProperties = ImmutableMap.of();

    private Schema schema;

    private Builder() {}

    /**
     * Sets the identifier of the table this context is about.
     *
     * @param tableIdentifier the table identifier
     * @return this builder
     */
    public Builder withTableIdentifier(NameIdentifier tableIdentifier) {
      this.tableIdentifier = tableIdentifier;
      return this;
    }

    /**
     * Sets the properties of the table this context is about. A null map is treated as an empty
     * one; a null value under a key is kept as it is.
     *
     * @param tableProperties the table properties
     * @return this builder
     */
    public Builder withTableProperties(Map<String, String> tableProperties) {
      // A defensive copy that tolerates null values, which ImmutableMap.copyOf would reject. The
      // catalog itself accepts a property whose value is null, and building this context must not
      // be what turns such a request into a failure.
      this.tableProperties =
          tableProperties == null
              ? ImmutableMap.of()
              : Collections.unmodifiableMap(Maps.newHashMap(tableProperties));
      return this;
    }

    /**
     * Sets the schema the table belongs to.
     *
     * @param schema the parent schema
     * @return this builder
     */
    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Builds the {@link TableLocationContext}.
     *
     * @return the built context
     */
    public TableLocationContext build() {
      Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
      Preconditions.checkArgument(schema != null, "schema must not be null");
      return new TableLocationContext(tableIdentifier, tableProperties, schema);
    }
  }
}
