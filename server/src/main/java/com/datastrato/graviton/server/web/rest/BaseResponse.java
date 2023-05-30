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

  private static final BaseResponse OK = new BaseResponse(0, null, null);

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

  public static BaseResponse ok() {
    return OK;
  }

  public static BaseResponse illegalArguments(String message) {
    return new BaseResponse(1001, "Illegal Arguments", message);
  }

  public static BaseResponse internalError(String message) {
    return new BaseResponse(1002, "Internal Error", message);
  }
}
