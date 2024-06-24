/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.meta;

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

public class EntityMetadata {

  private Long id;

  private String name;

  @Nullable private String comment;

  @Nullable private Map<String, String> properties;

  public Long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getComment() {
    return comment;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public EntityMetadata(Long id, String name, String comment, Map<String, String> properties) {
    this.id = id;
    this.name = name;
    this.comment = comment;
    this.properties = properties;
  }

  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (!(obj instanceof EntityMetadata)) return false;

    EntityMetadata that = (EntityMetadata) obj;
    return Objects.equals(this.id, that.id)
        && Objects.equals(this.name, that.name)
        && Objects.equals(this.comment, that.comment)
        && Objects.equals(this.properties, that.properties);
  }

  public int hashCode() {
    return Objects.hash(id, name, comment, properties);
  }
}
