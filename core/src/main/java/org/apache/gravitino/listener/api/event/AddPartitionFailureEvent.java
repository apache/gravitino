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
import org.apache.gravitino.listener.api.info.partitions.PartitionInfo;

/**
 * Represents an event that is generated when an attempt to add a partition fails due to an
 * exception.
 */
@DeveloperApi
public final class AddPartitionFailureEvent extends PartitionFailureEvent {
  private final PartitionInfo createdPartitionInfo;

  /**
   * Constructs a {@code AddPartitionFailureEvent} instance, capturing detailed information about
   * the failed add partition attempt.
   *
   * @param user The user who initiated the add partition operation.
   * @param identifier The identifier of the partition that was attempted to be created.
   * @param exception The exception that was thrown during the add partition operation, providing
   *     insight into what went wrong.
   * @param createdPartitionInfo The original request information used to attempt to add the
   *     partition. This includes details such as the intended partition schema, properties, and
   *     other configuration options that were specified.
   */
  public AddPartitionFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      PartitionInfo createdPartitionInfo) {
    super(user, identifier, exception);
    this.createdPartitionInfo = createdPartitionInfo;
  }

  /**
   * Retrieves the original request information for the attempted add partition.
   *
   * @return The {@link PartitionInfo} instance representing the request information for the failed
   *     add partition attempt.
   */
  public PartitionInfo createdPartitionInfo() {
    return createdPartitionInfo;
  }
}
