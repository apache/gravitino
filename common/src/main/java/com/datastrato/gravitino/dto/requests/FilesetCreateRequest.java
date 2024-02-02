/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@Getter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class FilesetCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private String name;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @JsonProperty("type")
  private Fileset.Type type;

  @Nullable
  @JsonProperty("storageLocation")
  private String storageLocation;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkNotNull(type, "\"type\" field is required and cannot be empty");
  }

  @Builder
  private static FilesetCreateRequest internalBuilder(
      String name,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties) {
    return new FilesetCreateRequest(
        name,
        comment,
        Optional.ofNullable(type).orElse(Fileset.Type.MANAGED),
        storageLocation,
        properties);
  }
}
