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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.gravitino.json.JsonUtils.PartitionDTODeserializer;
import org.apache.gravitino.json.JsonUtils.PartitionDTOSerializer;
import org.apache.gravitino.rel.partitions.Partition;

/** Represents a Partition Data Transfer Object (DTO) that implements the Partition interface. */
@JsonSerialize(using = PartitionDTOSerializer.class)
@JsonDeserialize(using = PartitionDTODeserializer.class)
public interface PartitionDTO extends Partition {

  /**
   * @return The type of the partition.
   */
  Type type();

  /** Type of the partition. */
  enum Type {
    /** The range partition type. */
    RANGE,

    /** The list partition type. */
    LIST,

    /** The identity partition type. */
    IDENTITY
  }
}
