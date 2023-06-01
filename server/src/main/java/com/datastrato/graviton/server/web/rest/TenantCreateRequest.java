package com.datastrato.graviton.server.web.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class TenantCreateRequest extends BaseRequest {

  @JsonProperty("name")
  public String name;

  @Nullable
  @JsonProperty("comment")
  public String comment;

  // Only for Jackson deserialization
  TenantCreateRequest() {
    this(null, null);
  }

  TenantCreateRequest(String name, String comment) {
    super();

    this.name = Optional.ofNullable(name).map(String::trim).orElse(null);
    this.comment = Optional.ofNullable(comment).map(String::trim).orElse(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "\"name\" field is required and cannot be empty");
  }
}
