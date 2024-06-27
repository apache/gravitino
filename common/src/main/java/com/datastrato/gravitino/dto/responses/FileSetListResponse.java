/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.file.FilesetDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a list of filesets with their information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class FileSetListResponse extends BaseResponse {

  @JsonProperty("filesets")
  private final FilesetDTO[] filesets;

  @JsonProperty("noExistsFilesets")
  private final String[] notExistsFilesets;

  /**
   * Creates a new FileSetListResponse.
   *
   * @param filesets The list of filesets.
   * @param notExistsFilesets The list of filesets.
   */
  public FileSetListResponse(FilesetDTO[] filesets, String[] notExistsFilesets) {
    super(0);
    this.filesets = filesets;
    this.notExistsFilesets = notExistsFilesets;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * FileSetListResponse.
   */
  public FileSetListResponse() {
    super();
    this.filesets = null;
    this.notExistsFilesets = null;
  }
}
