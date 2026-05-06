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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to update a view. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = ViewUpdateRequest.RenameViewRequest.class, name = "rename"),
  @JsonSubTypes.Type(value = ViewUpdateRequest.SetViewPropertyRequest.class, name = "setProperty"),
  @JsonSubTypes.Type(
      value = ViewUpdateRequest.RemoveViewPropertyRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(value = ViewUpdateRequest.ReplaceViewRequest.class, name = "replaceView")
})
public interface ViewUpdateRequest extends RESTRequest {

  /**
   * The view change represented by this request.
   *
   * @return An instance of {@link ViewChange}.
   */
  ViewChange viewChange();

  /** Represents a request to rename a view. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class RenameViewRequest implements ViewUpdateRequest {

    @JsonProperty("newName")
    private final String newName;

    /**
     * Constructor for RenameViewRequest.
     *
     * @param newName The new name of the view.
     */
    public RenameViewRequest(String newName) {
      this.newName = newName;
    }

    /** Default constructor for Jackson deserialization. */
    public RenameViewRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }

    @Override
    public ViewChange viewChange() {
      return ViewChange.rename(newName);
    }
  }

  /** Represents a request to set a property of a view. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class SetViewPropertyRequest implements ViewUpdateRequest {

    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    /**
     * Constructor for SetViewPropertyRequest.
     *
     * @param property The property to set.
     * @param value The value of the property.
     */
    public SetViewPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** Default constructor for Jackson deserialization. */
    public SetViewPropertyRequest() {
      this(null, null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }

    @Override
    public ViewChange viewChange() {
      return ViewChange.setProperty(property, value);
    }
  }

  /** Represents a request to remove a property of a view. */
  @EqualsAndHashCode
  @ToString
  @Getter
  class RemoveViewPropertyRequest implements ViewUpdateRequest {

    @JsonProperty("property")
    private final String property;

    /**
     * Constructor for RemoveViewPropertyRequest.
     *
     * @param property The property to remove.
     */
    public RemoveViewPropertyRequest(String property) {
      this.property = property;
    }

    /** Default constructor for Jackson deserialization. */
    public RemoveViewPropertyRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }

    @Override
    public ViewChange viewChange() {
      return ViewChange.removeProperty(property);
    }
  }

  /**
   * Represents a request to atomically replace the body (columns, representations, default catalog,
   * default schema and comment) of a view. View name and properties are not affected.
   */
  @EqualsAndHashCode
  @ToString
  @Getter
  class ReplaceViewRequest implements ViewUpdateRequest {

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

    @JsonProperty("comment")
    @Nullable
    private final String comment;

    /**
     * Constructor for ReplaceViewRequest.
     *
     * @param columns The new output columns of the view.
     * @param representations The new representations of the view.
     * @param defaultCatalog The new default catalog, or {@code null} to unset it.
     * @param defaultSchema The new default schema, or {@code null} to unset it.
     * @param comment The new comment, or {@code null} to unset it.
     */
    public ReplaceViewRequest(
        ColumnDTO[] columns,
        RepresentationDTO[] representations,
        @Nullable String defaultCatalog,
        @Nullable String defaultSchema,
        @Nullable String comment) {
      this.columns = columns;
      this.representations = representations;
      this.defaultCatalog = defaultCatalog;
      this.defaultSchema = defaultSchema;
      this.comment = comment;
    }

    /** Default constructor for Jackson deserialization. */
    public ReplaceViewRequest() {
      this(null, null, null, null, null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
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

    @Override
    public ViewChange viewChange() {
      return ViewChange.replaceView(
          columns == null ? new ColumnDTO[0] : columns,
          DTOConverters.fromDTOs(representations),
          defaultCatalog,
          defaultSchema,
          comment);
    }
  }
}
