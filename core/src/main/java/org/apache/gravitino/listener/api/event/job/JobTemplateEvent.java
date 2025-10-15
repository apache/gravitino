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
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.OperationStatus;

/**
 * Represents an abstract base class for events related to job template operations. This class
 * extends {@link Event} to provide a more specific context involving operations on job templates,
 * such as register, alter, or delete.
 */
@DeveloperApi
public abstract class JobTemplateEvent extends Event {

  /**
   * Constructs a new {@code JobTemplateEvent} with the specified user and job template identifier.
   *
   * @param user The user responsible for triggering the job template operation.
   * @param identifier The identifier of the job template involved in the operation.
   */
  protected JobTemplateEvent(String user, NameIdentifier identifier) {
    super(user, identifier);
  }

  @Override
  public OperationStatus operationStatus() {
    return OperationStatus.SUCCESS;
  }
}
