/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.rest.RESTResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class OAuth2ErrorResponse implements RESTResponse {
  @JsonProperty("error")
  private String type;

  @Nullable
  @JsonProperty("error_description")
  private String message;

  public OAuth2ErrorResponse(String type, String message) {
    this.type = type;
    this.message = message;
  }

  // This is the constructor that is used by Jackson deserializer
  public OAuth2ErrorResponse() {}

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(type != null, "OAuthErrorResponse should contain type");
  }
}
