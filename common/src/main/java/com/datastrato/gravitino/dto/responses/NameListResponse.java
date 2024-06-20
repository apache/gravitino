package com.datastrato.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a list of entity names. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class NameListResponse extends BaseResponse {

  @JsonProperty("names")
  private final String[] names;

  /**
   * Creates a new NameListResponse.
   *
   * @param names The list of names.
   */
  public NameListResponse(String[] names) {
    this.names = names;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * NameListResponse.
   */
  public NameListResponse() {
    this.names = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

  Preconditions.checkArgument(names != null, "names must not be null");
  }
}
