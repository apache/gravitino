/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/

package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = MetalakeUpdateRequest.RenameMetalakeRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = MetalakeUpdateRequest.UpdateMetalakeCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = MetalakeUpdateRequest.SetMetalakePropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = MetalakeUpdateRequest.RemoveMetalakePropertyRequest.class,
      name = "removeProperty")
})
public interface MetalakeUpdateRequest extends RESTRequest {

  MetalakeChange metalakeChange();

  @EqualsAndHashCode
  @ToString
  class RenameMetalakeRequest implements MetalakeUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    public RenameMetalakeRequest(String newName) {
      this.newName = newName;
    }

    public RenameMetalakeRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          newName != null && !newName.isEmpty(),
          "\"newName\" field is required and cannot be empty");
    }

    @Override
    public MetalakeChange metalakeChange() {
      return MetalakeChange.rename(newName);
    }
  }

  @EqualsAndHashCode
  @ToString
  class UpdateMetalakeCommentRequest implements MetalakeUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    public UpdateMetalakeCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    public UpdateMetalakeCommentRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          newComment != null && !newComment.isEmpty(),
          "\"newComment\" field is required and cannot be empty");
    }

    @Override
    public MetalakeChange metalakeChange() {
      return MetalakeChange.updateComment(newComment);
    }
  }

  @EqualsAndHashCode
  @ToString
  class SetMetalakePropertyRequest implements MetalakeUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    public SetMetalakePropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    public SetMetalakePropertyRequest() {
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
    public MetalakeChange metalakeChange() {
      return MetalakeChange.setProperty(property, value);
    }
  }

  @EqualsAndHashCode
  @ToString
  class RemoveMetalakePropertyRequest implements MetalakeUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    public RemoveMetalakePropertyRequest(String property) {
      this.property = property;
    }

    public RemoveMetalakePropertyRequest() {
      this(null);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          property != null && !property.isEmpty(),
          "\"property\" field is required and cannot be empty");
    }

    @Override
    public MetalakeChange metalakeChange() {
      return MetalakeChange.removeProperty(property);
    }
  }
}
