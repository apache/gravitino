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
package org.apache.gravitino.dto.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.messaging.DataLayout;
import org.apache.gravitino.messaging.DataLayouts;
import org.apache.gravitino.messaging.SchemaDataLayout;

/** REST/JSON representation of {@link DataLayout}. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataLayoutDTO implements DataLayout {

  @JsonProperty("format")
  private final Format format;

  @Nullable
  @JsonProperty("typeName")
  private final String typeName;

  @Nullable
  @JsonProperty("schemaId")
  private final String schemaId;

  @Nullable
  @JsonProperty("schemaVersion")
  private final String schemaVersion;

  @Nullable
  @JsonProperty("schemaUri")
  private final String schemaUri;

  @Nullable
  @JsonProperty("schemaSubject")
  private final String schemaSubject;

  @Nullable
  @JsonProperty("schemaText")
  private final String schemaText;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for Jackson. */
  public DataLayoutDTO() {
    this(Format.UNKNOWN, null, null, null, null, null, null, null);
  }

  /**
   * Creates a DataLayoutDTO.
   *
   * @param format payload format
   * @param typeName type / message FQN
   * @param schemaId registry schema id
   * @param schemaVersion registry version
   * @param schemaUri registry URL
   * @param schemaSubject registry subject
   * @param schemaText inline schema text
   * @param properties extra properties
   */
  public DataLayoutDTO(
      Format format,
      @Nullable String typeName,
      @Nullable String schemaId,
      @Nullable String schemaVersion,
      @Nullable String schemaUri,
      @Nullable String schemaSubject,
      @Nullable String schemaText,
      @Nullable Map<String, String> properties) {
    this.format = format == null ? Format.UNKNOWN : format;
    this.typeName = typeName;
    this.schemaId = schemaId;
    this.schemaVersion = schemaVersion;
    this.schemaUri = schemaUri;
    this.schemaSubject = schemaSubject;
    this.schemaText = schemaText;
    this.properties =
        properties == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(properties));
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
    return properties == null ? Collections.emptyMap() : properties;
  }

  /**
   * Converts an API {@link DataLayout} to a DTO.
   *
   * @param layout source layout
   * @return DTO or null
   */
  public static DataLayoutDTO fromDataLayout(DataLayout layout) {
    if (layout == null) {
      return null;
    }
    return DataLayoutDTO.builder()
        .format(layout.format())
        .typeName(layout.typeName())
        .schemaId(layout.schemaId())
        .schemaVersion(layout.schemaVersion())
        .schemaUri(layout.schemaUri())
        .schemaSubject(layout.schemaSubject())
        .schemaText(layout.schemaText())
        .properties(layout.properties().isEmpty() ? null : layout.properties())
        .build();
  }

  /**
   * Converts this DTO to an immutable {@link SchemaDataLayout}.
   *
   * @return SchemaDataLayout
   */
  public SchemaDataLayout toSchemaDataLayout() {
    return DataLayouts.toSchemaDataLayout(this);
  }

  /**
   * Validates the data layout contents.
   *
   * @throws IllegalArgumentException If the layout is completely empty, if a schema field is
   *     present but blank, if {@code schemaUri} is not a syntactically valid URI, or if {@code
   *     schemaText} is not valid JSON for the {@link Format#AVRO} or {@link Format#JSON} formats.
   */
  public void validate() throws IllegalArgumentException {
    boolean hasContent =
        format != Format.UNKNOWN
            || typeName != null
            || schemaId != null
            || schemaVersion != null
            || schemaUri != null
            || schemaSubject != null
            || schemaText != null
            || (properties != null && !properties.isEmpty());
    Preconditions.checkArgument(
        hasContent, "data layout must specify a format or at least one schema field");

    checkNotBlankIfPresent(typeName, "typeName");
    checkNotBlankIfPresent(schemaId, "schemaId");
    checkNotBlankIfPresent(schemaVersion, "schemaVersion");
    checkNotBlankIfPresent(schemaUri, "schemaUri");
    checkNotBlankIfPresent(schemaSubject, "schemaSubject");
    checkNotBlankIfPresent(schemaText, "schemaText");

    if (schemaUri != null) {
      try {
        new URI(schemaUri);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("\"schemaUri\" is not a valid URI: " + schemaUri, e);
      }
    }

    if (schemaText != null && (format == Format.AVRO || format == Format.JSON)) {
      try {
        JsonUtils.objectMapper().readTree(schemaText);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(
            "\"schemaText\" must be valid JSON for format "
                + format
                + ": "
                + e.getOriginalMessage(),
            e);
      }
    }
  }

  private static void checkNotBlankIfPresent(String value, String fieldName) {
    Preconditions.checkArgument(
        value == null || StringUtils.isNotBlank(value),
        "\"%s\" must not be blank when present",
        fieldName);
  }
}
