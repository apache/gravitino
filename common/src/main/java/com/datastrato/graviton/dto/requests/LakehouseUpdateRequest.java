package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.LakehouseChange;
import com.datastrato.graviton.dto.RESTRequest;
import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = LakehouseUpdateRequest.RenameLakehouseRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = LakehouseUpdateRequest.UpdateLakehouseCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = LakehouseUpdateRequest.SetLakehousePropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = LakehouseUpdateRequest.RemoveLakehousePropertyRequest.class,
      name = "removeProperty")
})
public interface LakehouseUpdateRequest extends RESTRequest {

  /** @return the {@link LakehouseChange} that this request represents */
  LakehouseChange lakehouseChange();

  @EqualsAndHashCode
  class RenameLakehouseRequest implements LakehouseUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    public RenameLakehouseRequest(String newName) {
      this.newName = newName;
    }

    public RenameLakehouseRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          newName != null && !newName.isEmpty(),
          "\"newName\" field is required and cannot be empty");
    }

    @Override
    public LakehouseChange lakehouseChange() {
      return LakehouseChange.rename(newName);
    }
  }

  @EqualsAndHashCode
  class UpdateLakehouseCommentRequest implements LakehouseUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    public UpdateLakehouseCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    public UpdateLakehouseCommentRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          newComment != null && !newComment.isEmpty(),
          "\"newComment\" field is required and cannot be empty");
    }

    @Override
    public LakehouseChange lakehouseChange() {
      return LakehouseChange.updateComment(newComment);
    }
  }

  @EqualsAndHashCode
  class SetLakehousePropertyRequest implements LakehouseUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    public SetLakehousePropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    public SetLakehousePropertyRequest() {
      this(null, null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          property != null && !property.isEmpty(),
          "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(
          value != null && !value.isEmpty(), "\"value\" field is required and cannot be empty");
    }

    @Override
    public LakehouseChange lakehouseChange() {
      return LakehouseChange.setProperty(property, value);
    }
  }

  @EqualsAndHashCode
  class RemoveLakehousePropertyRequest implements LakehouseUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    public RemoveLakehousePropertyRequest(String property) {
      this.property = property;
    }

    public RemoveLakehousePropertyRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          property != null && !property.isEmpty(),
          "\"property\" field is required and cannot be empty");
    }

    @Override
    public LakehouseChange lakehouseChange() {
      return LakehouseChange.removeProperty(property);
    }
  }
}
