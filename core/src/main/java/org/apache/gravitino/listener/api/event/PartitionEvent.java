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
 * Represents an abstract base class for events related to Partition operations. This class extends
 * {@link Event} to provide a more specific context involving operations on Partitions, such as
 * creation, deletion, or modification. It captures essential information including the user
 * performing the operation and the identifier of the Partition being operated on.
 */
@DeveloperApi
public abstract class PartitionEvent extends Event {
  /**
   * Constructs a new {@code PartitionEvent} with the specified user and Partition identifier.
   *
   * @param user The user responsible for triggering the Partition operation.
   * @param identifier The identifier of the Partition involved in the operation.
   */
  protected PartitionEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }
}
