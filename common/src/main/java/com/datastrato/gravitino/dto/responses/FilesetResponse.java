/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.file.FilesetDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class FilesetResponse extends BaseResponse {

  @JsonProperty("fileset")
  private final FilesetDTO fileset;

  public FilesetResponse() {
    super(0);
    this.fileset = null;
  }

  public FilesetResponse(FilesetDTO fileset) {
    super(0);
    this.fileset = fileset;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(fileset != null, "fileset must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileset.name()), "fileset 'name' must not be null and empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fileset.storageLocation()),
        "fileset 'storageLocation' must not be null and empty");
    Preconditions.checkNotNull(fileset.type(), "fileset 'type' must not be null and empty");
  }
}
