package com.datastrato.graviton.server.web.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class BaseResponse {

  @JsonProperty("code")
  private int code;

  @Nullable
  @JsonProperty("type")
  private String type;

  @Nullable
  @JsonProperty("message")
  private String message;

  BaseResponse(int code, String type, String message) {
    this.code = code;
    this.type = type;
    this.message = message;
  }

  public static BaseResponse error(ErrorType type, String message) {
    return new BaseResponse(type.errorCode(), type.type(), message);
  }
}
