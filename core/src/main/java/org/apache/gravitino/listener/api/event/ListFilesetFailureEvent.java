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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Represents an event that is triggered when an attempt to list filesets within a namespace fails.
 */
@DeveloperApi
public final class ListFilesetFailureEvent extends FilesetFailureEvent {
  private final Namespace namespace;

  /**
   * Constructs a new {@code ListFilesetFailureEvent}.
   *
   * @param user The username of the individual associated with the failed fileset listing
   *     operation.
   * @param namespace The namespace in which the fileset listing was attempted.
   * @param exception The exception encountered during the fileset listing attempt, which serves as
   *     an indicator of the issues that caused the failure.
   */
  public ListFilesetFailureEvent(String user, Namespace namespace, Exception exception) {
    super(user, NameIdentifier.of(namespace.levels()), exception);
    this.namespace = namespace;
  }

  /**
   * Retrieves the namespace associated with the failed listing event.
   *
   * @return The {@link Namespace} that was targeted during the failed listing operation.
   */
  public Namespace namespace() {
    return namespace;
  }
}
