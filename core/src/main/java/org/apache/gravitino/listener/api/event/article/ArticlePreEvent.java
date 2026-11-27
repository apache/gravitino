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
package org.apache.gravitino.listener.api.event.article;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.PreEvent;

/** A requested Knowledge Base Article operation. */
@DeveloperApi
public final class ArticlePreEvent extends PreEvent {
  private final OperationType operation;

  /**
   * Creates the event.
   *
   * @param user the initiating user
   * @param identifier the Article identifier, or the metalake for a collection operation
   * @param operation the Article operation
   */
  public ArticlePreEvent(String user, NameIdentifier identifier, OperationType operation) {
    super(user, identifier);
    this.operation = operation;
  }

  @Override
  public OperationType operationType() {
    return operation;
  }
}
