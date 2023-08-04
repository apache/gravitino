/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.rest.RESTResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class BaseResponse implements RESTResponse {

  @JsonProperty("code")
  private final int code;

  public BaseResponse(int code) {
    this.code = code;
  }

  // This is the constructor that is used by Jackson deserializer
  public BaseResponse() {
    this.code = 0;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(code >= 0, "code must be >= 0");
  }
}
