/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.tag;

import com.datastrato.gravitino.MetadataObject;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Represents a Metadata Object DTO (Data Transfer Object). */
public class MetadataObjectDTO implements MetadataObject {

  private String parent;

  private String name;

  @JsonProperty("type")
  private Type type;

  private MetadataObjectDTO() {}

  @Override
  public String parent() {
    return parent;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type type() {
    return type;
  }

  /** @return The full name of the metadata object. */
  @JsonProperty("fullName")
  public String getFullName() {
    return parent + "." + name;
  }

  /** Sets the full name of the metadata object. */
  @JsonProperty("fullName")
  public void setFullName(String fullName) {
    int index = fullName.lastIndexOf(".");
    if (index == -1) {
      parent = null;
      name = fullName;
    } else {
      parent = fullName.substring(0, index);
      name = fullName.substring(index + 1);
    }
  }

  /** @return a new builder for constructing a Metadata Object DTO. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for constructing a Metadata Object DTO. */
  public static class Builder {

    private final MetadataObjectDTO metadataObjectDTO = new MetadataObjectDTO();

    /**
     * Sets the parent of the metadata object.
     *
     * @param parent The parent of the metadata object.
     * @return The builder.
     */
    public Builder withParent(String parent) {
      metadataObjectDTO.parent = parent;
      return this;
    }

    /**
     * Sets the name of the metadata object.
     *
     * @param name The name of the metadata object.
     * @return The builder.
     */
    public Builder withName(String name) {
      metadataObjectDTO.name = name;
      return this;
    }

    /**
     * Sets the type of the metadata object.
     *
     * @param type The type of the metadata object.
     * @return The builder.
     */
    public Builder withType(Type type) {
      metadataObjectDTO.type = type;
      return this;
    }

    /** @return The constructed Metadata Object DTO. */
    public MetadataObjectDTO build() {
      return metadataObjectDTO;
    }
  }
}
