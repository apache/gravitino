package com.datastrato.unified_catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public final class VirtualTableInfo implements hasExtraInfo.ExtraInfo {

  public static final Field CONNECTION_ID =
      Field.required("connection_id", Integer.class, "The unique identifier of the connection");
  public static final Field IDENTIFIER =
      Field.required("identifier", List.class, "The unique identifier of the table");

  @JsonProperty("connection_id")
  private final Integer connectionId;

  @JsonProperty("identifier")
  private final List<String> identifier;

  // Only for Jackson deserialization
  private VirtualTableInfo() {
    this.connectionId = null;
    this.identifier = null;
  }

  public VirtualTableInfo(Integer connectionId, List<String> identifier) {
    this.connectionId = connectionId;
    this.identifier = identifier;
    this.validate();
  }

  @Override
  public Map<Field, Object> fields() {
    return new ImmutableMap.Builder<Field, Object>()
        .put(CONNECTION_ID, connectionId)
        .put(IDENTIFIER, identifier)
        .build();
  }
}
