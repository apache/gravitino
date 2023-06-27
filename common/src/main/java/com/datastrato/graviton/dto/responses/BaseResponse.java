package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.rest.RESTResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class BaseResponse implements RESTResponse {

  @JsonProperty("code")
  private int code;

  @Nullable
  @JsonProperty("type")
  private String type;

  @Nullable
  @JsonProperty("message")
  private String message;

  public BaseResponse(int code, String type, String message) {
    this.code = code;
    this.type = type;
    this.message = message;
  }

  // This is the constructor that is used by Jackson deserializer
  public BaseResponse() {
    this.code = 0;
    this.type = null;
    this.message = null;
  }

  public static BaseResponse error(ErrorType type, String message) {
    return new BaseResponse(type.errorCode(), type.errorType(), message);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(code >= 0, "code must be >= 0");
    Preconditions.checkArgument(
        type != null && !type.isEmpty(), "type must be non-null and non-empty");
    Preconditions.checkArgument(
        message != null && !message.isEmpty(), "message must be non-null and non-empty");
  }
}
