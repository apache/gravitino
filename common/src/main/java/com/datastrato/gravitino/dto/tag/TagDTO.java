/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.tag;

import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.tag.Tag;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;

/** Represents a Tag Data Transfer Object (DTO). */
@EqualsAndHashCode
public class TagDTO implements Tag, Tag.AssociatedObjects {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  @JsonProperty("inherited")
  private Optional<Boolean> inherited = Optional.empty();

  @JsonProperty("objects")
  private MetadataObjectDTO[] objects;

  private TagDTO() {}

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  @Override
  public Optional<Boolean> inherited() {
    return inherited;
  }

  @Override
  public MetadataObject[] objects() {
    return objects;
  }

  @Override
  public AssociatedObjects associatedObjects() {
    return this;
  }

  /** @return a new builder for constructing a Tag DTO. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for constructing TagDTO instances. */
  public static class Builder {
    private final TagDTO tagDTO;

    private Builder() {
      tagDTO = new TagDTO();
    }

    /**
     * Sets the name of the tag.
     *
     * @param name The name of the tag.
     * @return The builder instance.
     */
    public Builder withName(String name) {
      tagDTO.name = name;
      return this;
    }

    /**
     * Sets the comment associated with the tag.
     *
     * @param comment The comment associated with the tag.
     * @return The builder instance.
     */
    public Builder withComment(String comment) {
      tagDTO.comment = comment;
      return this;
    }

    /**
     * Sets the properties associated with the tag.
     *
     * @param properties The properties associated with the tag.
     * @return The builder instance.
     */
    public Builder withProperties(Map<String, String> properties) {
      tagDTO.properties = properties;
      return this;
    }

    /**
     * Sets the audit information for the tag.
     *
     * @param audit The audit information for the tag.
     * @return The builder instance.
     */
    public Builder withAudit(AuditDTO audit) {
      tagDTO.audit = audit;
      return this;
    }

    /**
     * Sets whether the tag is inherited.
     *
     * @param inherited Whether the tag is inherited.
     * @return The builder instance.
     */
    public Builder withInherited(Optional<Boolean> inherited) {
      tagDTO.inherited = inherited;
      return this;
    }

    /**
     * Sets the objects associated with the tag.
     *
     * @param objects The objects associated with the tag.
     * @return The builder instance.
     */
    public Builder withObjects(MetadataObjectDTO[] objects) {
      tagDTO.objects = objects;
      return this;
    }

    /** @return The constructed Tag DTO. */
    public TagDTO build() {
      return tagDTO;
    }
  }
}
