package com.datastrato.graviton.schema;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** Interface for entities that have extra info. */
public interface HasExtraInfo {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes({@JsonSubTypes.Type(value = VirtualTableInfo.class, name = "VIRTUAL")})
  interface ExtraInfo extends Entity {}

  /**
   * Return the extra info of the entity.
   *
   * @return the extra info of the entity.
   */
  ExtraInfo extraInfo();
}
