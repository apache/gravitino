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

/**
 * Represents an event that is activated upon the successful check existing operation of a
 * partition.
 */
@DeveloperApi
public class PartitionExistsEvent extends PartitionEvent {
  private final boolean isExists;
  private final String partitionName;

  /**
   * Constructs an instance of {@code PartitionExistsEvent}, capturing essential details about the
   * successful get a partition.
   *
   * @param user The username of the individual who initiated the check existion operation of a
   *     partition.
   * @param identifier The unique identifier of the partition that was gotten.
   * @param isExists A boolean indicator reflecting whether the partition was present in the table
   *     at the time of the check existing operation.
   * @param partitionName The name of the partition.
   */
  public PartitionExistsEvent(
      String user, NameIdentifier identifier, boolean isExists, String partitionName) {
    super(user, identifier);
    this.isExists = isExists;
    this.partitionName = partitionName;
  }

  /**
   * Retrieves the status of the partition's existence at the time of the exist check operation.
   *
   * @return A boolean value indicating the partition's existence status. {@code true} signifies
   *     that the partition was present before the operation, {@code false} indicates it was not.
   */
  public boolean isExists() {
    return isExists;
  }

  /**
   * Retrieves the existence status of the partition at the time of the exist check operation.
   *
   * @return A string value indicating the name of the partition.
   */
  public String partitionName() {
    return partitionName;
  }
}
