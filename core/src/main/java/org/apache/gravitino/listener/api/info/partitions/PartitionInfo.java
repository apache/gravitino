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

package org.apache.gravitino.listener.api.info.partitions;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.RangePartition;

/**
 * Provides access to metadata about a Partition instance, designed for use by event listeners. This
 * class encapsulates the essential attributes of a Partition, including its name and properties
 * information.
 */
@DeveloperApi
public abstract class PartitionInfo {
  private String name;
  private Map<String, String> properties;

  /**
   * Directly constructs PartitionInfo with specified details.
   *
   * @param name The name of the Partition.
   * @param properties A map of properties associated with the Partition.
   */
  protected PartitionInfo(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
  }

  /**
   * Returns the name of the Partition.
   *
   * @return The Partition's name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the properties of the Partition.
   *
   * @return A map of Partition properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the instance of the PartitionInfo.
   *
   * @param partition the source {@link Partition} to convert into a {@link PartitionInfo}
   * @return A instance of the {@code PartitionInfo}
   * @throws GravitinoRuntimeException if the partition type is unrecognized
   */
  public static PartitionInfo of(Partition partition) {
    if (partition instanceof IdentityPartition) {
      IdentityPartition identityPartition = (IdentityPartition) partition;
      return new IdentityPartitionInfo(
          identityPartition.name(),
          identityPartition.fieldNames(),
          identityPartition.values(),
          identityPartition.properties());
    } else if (partition instanceof ListPartition) {
      ListPartition listPartition = (ListPartition) partition;
      return new ListPartitionInfo(
          listPartition.name(), listPartition.properties(), listPartition.lists());
    } else if (partition instanceof RangePartition) {
      RangePartition rangePartition = (RangePartition) partition;
      return new RangePartitionInfo(
          rangePartition.name(),
          rangePartition.upper(),
          rangePartition.lower(),
          rangePartition.properties());
    } else {
      throw new GravitinoRuntimeException("Invalid instance of PartitionInfo");
    }
  }
}
