/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/

package com.datastrato.graviton.dto.rel;

import com.datastrato.graviton.json.JsonUtils;
import com.datastrato.graviton.rel.Column;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import io.substrait.type.Type;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class ColumnDTO implements Column {

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type dataType;

  @JsonProperty("comment")
  private String comment;

  private ColumnDTO() {}

  private ColumnDTO(String name, Type dataType, String comment) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type dataType() {
    return dataType;
  }

  @Override
  public String comment() {
    return comment;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder<S extends Builder> {

    protected String name;
    protected Type dataType;
    protected String comment;

    public Builder() {}

    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    public S withDataType(Type dataType) {
      this.dataType = dataType;
      return (S) this;
    }

    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    public ColumnDTO build() {
      Preconditions.checkNotNull(name, "Column name cannot be null");
      Preconditions.checkNotNull(dataType, "Column data type cannot be null");
      return new ColumnDTO(name, dataType, comment);
    }
  }
}
