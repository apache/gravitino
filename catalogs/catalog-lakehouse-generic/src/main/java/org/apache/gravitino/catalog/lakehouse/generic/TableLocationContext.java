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
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;

/**
 * The context passed to a {@link TableLocationProvider}, both when the location of a table that is
 * being created has to be provisioned and when the location of a table that has been dropped has to
 * be unprovisioned.
 *
 * <p>The same type is used for both operations so that a new input becomes an additional accessor
 * rather than a signature change: providers written against an older version keep compiling and
 * running. For that reason implementations should not rely on the constructor signature and should
 * build instances through {@link #builder()}.
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
   * Returns the properties of the table this context is about. On provisioning, these are the
   * properties of the creation request, before the location is filled in. On unprovisioning, these
   * are the stored properties of the table being dropped, read right before it was removed, so they
   * carry the {@code location} that was provisioned for it under {@link
   * org.apache.gravitino.rel.Table#PROPERTY_LOCATION}.
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
     * Sets the properties of the table this context is about. A null value is treated as an empty
     * map.
     *
     * @param tableProperties the table properties
     * @return this builder
     */
    public Builder withTableProperties(Map<String, String> tableProperties) {
      this.tableProperties =
          tableProperties == null ? ImmutableMap.of() : ImmutableMap.copyOf(tableProperties);
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
