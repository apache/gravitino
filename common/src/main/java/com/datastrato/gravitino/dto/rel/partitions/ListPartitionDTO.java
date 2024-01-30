/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.rel.partitions.ListPartition;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class ListPartitionDTO implements PartitionDTO, ListPartition {

  private final String name;
  private final LiteralDTO[][] lists;
  private final Map<String, String> properties;

  public static Builder builder() {
    return new Builder();
  }

  private ListPartitionDTO() {
    this(null, null, null);
  }

  private ListPartitionDTO(String name, LiteralDTO[][] lists, Map<String, String> properties) {
    this.name = name;
    this.lists = lists;
    this.properties = properties;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public LiteralDTO[][] lists() {
    return lists;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public Type type() {
    return Type.LIST;
  }

  public static class Builder {
    private String name;
    private LiteralDTO[][] lists;
    private Map<String, String> properties;

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withLists(LiteralDTO[][] lists) {
      this.lists = lists;
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public ListPartitionDTO build() {
      return new ListPartitionDTO(name, lists, properties);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ListPartitionDTO that = (ListPartitionDTO) o;
    return Objects.equals(name, that.name)
        && Arrays.deepEquals(lists, that.lists)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name, properties);
    result = 31 * result + Arrays.deepHashCode(lists);
    return result;
  }
}
