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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to create a view. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ViewCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("comment")
  @Nullable
  private final String comment;

  @JsonProperty("columns")
  private final ColumnDTO[] columns;

  @JsonProperty("representations")
  private final RepresentationDTO[] representations;

  @JsonProperty("defaultCatalog")
  @Nullable
  private final String defaultCatalog;

  @JsonProperty("defaultSchema")
  @Nullable
  private final String defaultSchema;

  @JsonProperty("properties")
  @Nullable
  private final Map<String, String> properties;

  /** Default constructor for Jackson deserialization. */
  public ViewCreateRequest() {
    this(null, null, null, null, null, null, null);
  }

  /**
   * Creates a new {@link ViewCreateRequest}.
   *
   * @param name The name of the view.
   * @param comment The comment of the view.
   * @param columns The output columns of the view.
   * @param representations The representations of the view.
   * @param defaultCatalog The default catalog used to resolve unqualified identifiers in view
   *     representations.
   * @param defaultSchema The default schema used to resolve unqualified identifiers in view
   *     representations.
   * @param properties The properties of the view.
   */
  public ViewCreateRequest(
      String name,
      @Nullable String comment,
      ColumnDTO[] columns,
      RepresentationDTO[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      @Nullable Map<String, String> properties) {
    this.name = name;
    this.comment = comment;
    this.columns = columns;
    this.representations = representations;
    this.defaultCatalog = defaultCatalog;
    this.defaultSchema = defaultSchema;
    this.properties = properties;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(
        representations != null && representations.length > 0,
        "\"representations\" field is required and cannot be empty");
    Arrays.stream(representations)
        .forEach(
            rep -> {
              Preconditions.checkArgument(rep != null, "representation must not be null");
              rep.validate();
            });

    Set<String> seenDialects = new HashSet<>();
    Arrays.stream(representations)
        .filter(rep -> rep instanceof SQLRepresentationDTO)
        .map(rep -> ((SQLRepresentationDTO) rep).dialect())
        .forEach(
            dialect ->
                Preconditions.checkArgument(
                    seenDialects.add(dialect),
                    "Duplicate SQL representation dialect: %s",
                    dialect));
  }
}
