/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.file;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.file.Fileset;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/** Represents a Fileset DTO (Data Transfer Object). */
@NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FilesetDTO implements Fileset {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("type")
  private Type type;

  @JsonProperty("storageLocation")
  private String storageLocation;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

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
  public String storageLocation() {
    return storageLocation;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  @Builder(builderMethodName = "builder")
  private static FilesetDTO internalBuilder(
      String name,
      String comment,
      Type type,
      String storageLocation,
      Map<String, String> properties,
      AuditDTO audit) {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
    Preconditions.checkNotNull(type, "type cannot be null");
    Preconditions.checkNotNull(audit, "audit cannot be null");

    return new FilesetDTO(name, comment, type, storageLocation, properties, audit);
  }
}
