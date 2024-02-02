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

@EqualsAndHashCode
@ToString
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FilesetCreateRequest implements RESTRequest {

  @Getter
  @JsonProperty("name")
  private String name;

  @Getter
  @Nullable
  @JsonProperty("comment")
  private String comment;

  @JsonProperty("type")
  private Fileset.Type type;

  @Getter
  @Nullable
  @JsonProperty("storageLocation")
  private String storageLocation;

  @Getter
  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
  }

  public Fileset.Type getType() {
    return Optional.ofNullable(type).orElse(Fileset.Type.MANAGED);
  }
}
