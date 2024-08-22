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

/** Represents an event that is activated upon the successful creation of a partition. */
@DeveloperApi
public class AddPartitionEvent extends PartitionEvent {
  private final PartitionInfo createdPartitionInfo;

  /**
   * Constructs an instance of {@code AddPartitionEvent}, capturing essential details about the
   * successful creation of a partition.
   *
   * @param user The username of the individual who initiated the add partition.
   * @param identifier The unique identifier of the partition that was created.
   * @param createdPartitionInfo The final state of the partition post-creation.
   */
  public AddPartitionEvent(
      String user, NameIdentifier identifier, PartitionInfo createdPartitionInfo) {
    super(user, identifier);
    this.createdPartitionInfo = createdPartitionInfo;
  }

  /**
   * Provides the final state of the partition as it is presented to the user following the
   * successful creation.
   *
   * @return A {@link PartitionInfo} object that encapsulates the detailed characteristics of the
   *     newly created partition.
   */
  public PartitionInfo createdPartitionInfo() {
    return createdPartitionInfo;
  }
}
