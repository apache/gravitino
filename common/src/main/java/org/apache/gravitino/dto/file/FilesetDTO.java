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
package org.apache.gravitino.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.file.Fileset;

/** Represents a Fileset DTO (Data Transfer Object). */
@EqualsAndHashCode
public class FilesetDTO implements Fileset {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("type")
  private Type type;

  @JsonProperty("storageLocation")
  private String storageLocation;

  @JsonProperty("storageLocations")
  private Map<String, String> storageLocations;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  private FilesetDTO() {}

  private FilesetDTO(
      String name,
      String comment,
      Type type,
      String storageLocation,
      Map<String, String> storageLocations,
      Map<String, String> properties,
      AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.type = type;
    this.storageLocation = storageLocation;
    this.storageLocations = storageLocations;
    this.properties = properties;
    this.audit = audit;
  }

  @Override
  public String name() {
    return name;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public Map<String, String> storageLocations() {
    return storageLocations;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * Create a new FilesetDTO builder.
   *
   * @return A new FilesetDTO builder.
   */
  public static FilesetDTOBuilder builder() {
    return new FilesetDTOBuilder();
  }

  /** Builder for FilesetDTO. */
  public static class FilesetDTOBuilder {
    private String name;
    private String comment;
    private Type type;
    private Map<String, String> storageLocations = new HashMap<>();
    private Map<String, String> properties;
    private AuditDTO audit;

    private FilesetDTOBuilder() {}

    /**
     * Set the name of the fileset.
     *
     * @param name The name of the fileset.
     * @return The builder instance.
     */
    public FilesetDTOBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the comment of the fileset.
     *
     * @param comment The comment of the fileset.
     * @return The builder instance.
     */
    public FilesetDTOBuilder comment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Set the type of the fileset.
     *
     * @param type The type of the fileset.
     * @return The builder instance.
     */
    public FilesetDTOBuilder type(Type type) {
      this.type = type;
      return this;
    }

    /**
     * Set the storage locations of the fileset.
     *
     * @param storageLocations The storage locations of the fileset.
     * @return The builder instance.
     */
    public FilesetDTOBuilder storageLocations(Map<String, String> storageLocations) {
      this.storageLocations = ImmutableMap.copyOf(storageLocations);
      return this;
    }

    /**
     * Set the properties of the fileset.
     *
     * @param properties The properties of the fileset.
     * @return The builder instance.
     */
    public FilesetDTOBuilder properties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Set the audit information of the fileset.
     *
     * @param audit The audit information of the fileset.
     * @return The builder instance.
     */
    public FilesetDTOBuilder audit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /**
     * Build the FilesetDTO.
     *
     * @return The built FilesetDTO.
     */
    public FilesetDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(
          !storageLocations.isEmpty(),
          "storage locations cannot be empty. At least one location is required.");
      storageLocations.forEach(
          (n, l) -> {
            Preconditions.checkArgument(StringUtils.isNotBlank(n), "location name cannot be empty");
            Preconditions.checkArgument(
                StringUtils.isNotBlank(l), "storage location cannot be empty");
          });
      return new FilesetDTO(
          name,
          comment,
          type,
          storageLocations.get(LOCATION_NAME_UNKNOWN),
          storageLocations,
          properties,
          audit);
    }
  }
}
