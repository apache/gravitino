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

package org.apache.gravitino.listener.api.event.job;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.FailureEvent;

/**
 * Represents an event triggered when an attempt to perform a job operation fails due to an
 * exception.
 */
@DeveloperApi
public abstract class JobFailureEvent extends FailureEvent {

  /**
   * Constructs a new {@code JobFailureEvent} instance.
   *
   * @param user The user who initiated the job operation.
   * @param identifier The identifier of the job involved in the operation.
   * @param exception The exception encountered during the job operation, providing insights into
   *     the reasons behind the failure.
   */
  protected JobFailureEvent(String user, NameIdentifier identifier, Exception exception) {
    super(user, identifier, exception);
  }
}
