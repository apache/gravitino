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

/** Represents an event that is triggered upon the successful list of partitions. */
@DeveloperApi
public final class ListPartitionEvent extends PartitionEvent {
  /**
   * Constructs an instance of {@code ListPartitionEvent}.
   *
   * @param user The username of the individual who initiated the partition listing.
   * @param ident The identifier from which partitions were listed.
   */
  public ListPartitionEvent(String user, NameIdentifier ident) {
    super(user, ident);
  }
}
