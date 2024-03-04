/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTMessage;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/** Request to represent updates to a fileset. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(force = true)
@AllArgsConstructor
@ToString
public class FilesetUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<FilesetUpdateRequest> updates;

  @Override
  public void validate() throws IllegalArgumentException {
    updates.forEach(RESTMessage::validate);
  }
}
