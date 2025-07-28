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
 * Represents an event that is generated when an attempt to get a partition fails due to an
 * exception.
 */
@DeveloperApi
public final class PartitionExistsFailureEvent extends PartitionFailureEvent {
  private final String partitionName;

  /**
   * Constructs a {@code PartitionExistsFailureEvent} instance, capturing detailed information about
   * the failed partition exist check attempt.
   *
   * @param user The user who initiated the partition exist check operation.
   * @param identifier The identifier of the partition that was attempted to be gotten.
   * @param exception The exception that was thrown during the partition exist check operation,
   *     providing insight into what went wrong.
   * @param partitionName The name of the partition being checked for existence.
   */
  public PartitionExistsFailureEvent(
      String user, NameIdentifier identifier, Exception exception, String partitionName) {
    super(user, identifier, exception);
    this.partitionName = partitionName;
  }

  /**
   * Retrieves the existence status of the partition at the time of the exist check operation.
   *
   * @return A string value indicating the name of the partition.
   */
  public String partitionName() {
    return partitionName;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.PARTITION_EXISTS;
  }
}
