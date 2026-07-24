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
package org.apache.gravitino.messaging;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/**
 * Immutable {@link DataLayout} implementation used by Gravitino for a single named topic payload
 * schema (for example {@link DataLayouts#KEY} or {@link DataLayouts#VALUE}).
 *
 * <p>Create instances via {@link #builder()}.
 */
@Evolving
public final class SchemaDataLayout implements DataLayout {

  private final Format format;
  private final String typeName;
  private final String schemaId;
  private final String schemaVersion;
  private final String schemaUri;
  private final String schemaSubject;
  private final String schemaText;
  private final Map<String, String> properties;

  private SchemaDataLayout(
      Format format,
      String typeName,
      String schemaId,
      String schemaVersion,
      String schemaUri,
      String schemaSubject,
      String schemaText,
      Map<String, String> properties) {
    this.format = format == null ? Format.UNKNOWN : format;
    this.typeName = typeName;
    this.schemaId = schemaId;
    this.schemaVersion = schemaVersion;
    this.schemaUri = schemaUri;
    this.schemaSubject = schemaSubject;
    this.schemaText = schemaText;
    this.properties =
        properties == null || properties.isEmpty()
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(properties));
  }

  /**
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public Format format() {
    return format;
  }

  @Nullable
  @Override
  public String typeName() {
    return typeName;
  }

  @Nullable
  @Override
  public String schemaId() {
    return schemaId;
  }

  @Nullable
  @Override
  public String schemaVersion() {
    return schemaVersion;
  }

  @Nullable
  @Override
  public String schemaUri() {
    return schemaUri;
  }

  @Nullable
  @Override
  public String schemaSubject() {
    return schemaSubject;
  }

  @Nullable
  @Override
  public String schemaText() {
    return schemaText;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchemaDataLayout)) {
      return false;
    }
    SchemaDataLayout that = (SchemaDataLayout) o;
    return format == that.format
        && Objects.equals(typeName, that.typeName)
        && Objects.equals(schemaId, that.schemaId)
        && Objects.equals(schemaVersion, that.schemaVersion)
        && Objects.equals(schemaUri, that.schemaUri)
        && Objects.equals(schemaSubject, that.schemaSubject)
        && Objects.equals(schemaText, that.schemaText)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        format,
        typeName,
        schemaId,
        schemaVersion,
        schemaUri,
        schemaSubject,
        schemaText,
        properties);
  }

  @Override
  public String toString() {
    return "SchemaDataLayout{"
        + "format="
        + format
        + ", typeName='"
        + typeName
        + '\''
        + ", schemaId='"
        + schemaId
        + '\''
        + ", schemaVersion='"
        + schemaVersion
        + '\''
        + ", schemaUri='"
        + schemaUri
        + '\''
        + ", schemaSubject='"
        + schemaSubject
        + '\''
        + ", schemaText="
        + (schemaText == null ? "null" : ("<" + schemaText.length() + " chars>"))
        + ", properties="
        + properties
        + '}';
  }

  /** Builder for {@link SchemaDataLayout}. */
  public static final class Builder {
    private Format format = Format.UNKNOWN;
    private String typeName;
    private String schemaId;
    private String schemaVersion;
    private String schemaUri;
    private String schemaSubject;
    private String schemaText;
    private Map<String, String> properties;

    private Builder() {}

    /**
     * Sets the payload format.
     *
     * @param format the format; null is treated as {@link Format#UNKNOWN}
     * @return this builder
     */
    public Builder withFormat(Format format) {
      this.format = format;
      return this;
    }

    /**
     * Sets the fully-qualified type / record name.
     *
     * @param typeName type name, or null
     * @return this builder
     */
    public Builder withTypeName(String typeName) {
      this.typeName = typeName;
      return this;
    }

    /**
     * Sets the schema registry id.
     *
     * @param schemaId schema id, or null
     * @return this builder
     */
    public Builder withSchemaId(String schemaId) {
      this.schemaId = schemaId;
      return this;
    }

    /**
     * Sets the schema version string.
     *
     * @param schemaVersion schema version, or null
     * @return this builder
     */
    public Builder withSchemaVersion(String schemaVersion) {
      this.schemaVersion = schemaVersion;
      return this;
    }

    /**
     * Sets the schema registry / location URI.
     *
     * @param schemaUri schema URI, or null
     * @return this builder
     */
    public Builder withSchemaUri(String schemaUri) {
      this.schemaUri = schemaUri;
      return this;
    }

    /**
     * Sets the schema registry subject name.
     *
     * @param schemaSubject subject name, or null
     * @return this builder
     */
    public Builder withSchemaSubject(String schemaSubject) {
      this.schemaSubject = schemaSubject;
      return this;
    }

    /**
     * Sets inline schema text.
     *
     * @param schemaText schema text, or null
     * @return this builder
     */
    public Builder withSchemaText(String schemaText) {
      this.schemaText = schemaText;
      return this;
    }

    /**
     * Sets additional vendor-specific properties. The map is defensively copied on {@link
     * #build()}.
     *
     * @param properties properties, or null
     * @return this builder
     */
    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Builds an immutable {@link SchemaDataLayout}.
     *
     * @return the layout
     */
    public SchemaDataLayout build() {
      return new SchemaDataLayout(
          format,
          typeName,
          schemaId,
          schemaVersion,
          schemaUri,
          schemaSubject,
          schemaText,
          properties);
    }
  }
}
