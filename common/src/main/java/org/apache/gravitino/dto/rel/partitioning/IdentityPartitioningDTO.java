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
package org.apache.gravitino.dto.rel.partitioning;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.ArrayUtils;

/** Represents the identity partitioning. */
@EqualsAndHashCode(callSuper = true)
public final class IdentityPartitioningDTO extends Partitioning.SingleFieldPartitioning {

  /**
   * Creates a new IdentityPartitioningDTO.
   *
   * @param fieldName The name of the field to partition.
   * @return The new IdentityPartitioningDTO.
   */
  public static IdentityPartitioningDTO of(String... fieldName) {
    return new IdentityPartitioningDTO(fieldName);
  }

  private IdentityPartitioningDTO(String[] fieldName) {
    Preconditions.checkArgument(
        ArrayUtils.isNotEmpty(fieldName), "fieldName cannot be null or empty");
    this.fieldName = fieldName;
  }

  /**
   * @return The strategy of the partitioning.
   */
  @Override
  public Strategy strategy() {
    return Strategy.IDENTITY;
  }
}
