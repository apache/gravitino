/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.dto.rel.partitions;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.rel.partitions.ListPartition;

/**
 * Represents a List Partition Data Transfer Object (DTO) that implements the ListPartition
 * interface.
 */
public class ListPartitionDTO implements PartitionDTO, ListPartition {

  private final String name;
  private final LiteralDTO[][] lists;
  private final Map<String, String> properties;

  /**
   * @return The builder for ListPartitionDTO.
   */
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

  /**
   * @return The name of the partition.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The lists of the partition.
   */
  @Override
  public LiteralDTO[][] lists() {
    return lists;
  }

  /**
   * @return The properties of the partition.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * @return The type of the partition.
   */
  @Override
  public Type type() {
    return Type.LIST;
  }

  /** The builder for ListPartitionDTO. */
  public static class Builder {
    private String name;
    private LiteralDTO[][] lists;
    private Map<String, String> properties;

    /**
     * Set the name of the partition for the builder.
     *
     * @param name The name of the partition.
     * @return The builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the lists of the partition for the builder.
     *
     * @param lists The lists of the partition.
     * @return The builder.
     */
    public Builder withLists(LiteralDTO[][] lists) {
      this.lists = lists;
      return this;
    }

    /**
     * Set the properties of the partition for the builder.
     *
     * @param properties The properties of the partition.
     * @return The builder.
     */
    public Builder withProperties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Build the ListPartitionDTO.
     *
     * @return The ListPartitionDTO.
     */
    public ListPartitionDTO build() {
      return new ListPartitionDTO(name, lists, properties);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ListPartitionDTO)) {
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
