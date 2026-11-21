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

/**
 * The context passed to the schema callbacks of a {@link LocationProvider}: when a schema is being
 * created, and when a schema is being dropped.
 *
 * <p>As with {@link TableLocationContext}, one type serves both callbacks so that a new input
 * becomes an additional accessor rather than a signature change. Build instances through {@link
 * #builder()}.
 */
public class SchemaLocationContext {

  private final NameIdentifier schemaIdentifier;

  private final Map<String, String> schemaProperties;

  private SchemaLocationContext(
      NameIdentifier schemaIdentifier, Map<String, String> schemaProperties) {
    this.schemaIdentifier = schemaIdentifier;
    this.schemaProperties = schemaProperties;
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
   *   <li>{@link LocationProvider#provisionSchemaLocation(SchemaLocationContext)} gets the
   *       properties of the creation request, so the {@code location} entry holds whatever the
   *       caller supplied, which may be nothing at all.
   *   <li>{@link LocationProvider#unprovisionSchemaLocation(SchemaLocationContext)} gets the stored
   *       properties of the schema being dropped, so the {@code location} entry holds the location
   *       that was stored for it.
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
     * Builds the {@link SchemaLocationContext}.
     *
     * @return the built context
     */
    public SchemaLocationContext build() {
      Preconditions.checkArgument(schemaIdentifier != null, "schemaIdentifier must not be null");
      return new SchemaLocationContext(schemaIdentifier, schemaProperties);
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
