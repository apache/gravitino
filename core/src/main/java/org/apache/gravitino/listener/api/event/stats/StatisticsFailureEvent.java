/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.listener.api.event.stats;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.FailureEvent;

/** Base class for all statistics-related failure events. */
@DeveloperApi
public abstract class StatisticsFailureEvent extends FailureEvent {
  /**
   * Constructor for creating a statistics failure event.
   *
   * @param user the user initiating the event
   * @param identifier the name identifier associated with the event.
   * @param cause the exception that caused the failure
   */
  protected StatisticsFailureEvent(String user, NameIdentifier identifier, Exception cause) {
    super(user, identifier, cause);
  }
}
