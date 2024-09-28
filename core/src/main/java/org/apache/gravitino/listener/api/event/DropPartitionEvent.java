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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event that is generated after a partition is successfully dropped. */
@DeveloperApi
public final class DropPartitionEvent extends PartitionEvent {
  private final boolean isExists;
  private final String partitionName;

  /**
   * Constructs a new {@code DropPartitionEvent} instance, encapsulating information about the
   * outcome of a partition drop operation.
   *
   * @param user The user who initiated the drop partition operation.
   * @param identifier The identifier of the partition that was attempted to be dropped.
   * @param isExists A boolean flag indicating whether the partition existed at the time of the drop
   *     operation.
   * @param partitionName The name of the partition.
   */
  public DropPartitionEvent(
      String user, NameIdentifier identifier, boolean isExists, String partitionName) {
    super(user, identifier);
    this.isExists = isExists;
    this.partitionName = partitionName;
  }

  /**
   * Retrieves the existence status of the partition at the time of the drop operation.
   *
   * @return A boolean value indicating whether the partition existed. {@code true} if the partition
   *     existed, otherwise {@code false}.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Retrieves the existence status of the partition at the time of the drop operation.
   *
   * @return A string value indicating the name of the partition.
   */
  public String partitionName() {
    return partitionName;
  }
}
