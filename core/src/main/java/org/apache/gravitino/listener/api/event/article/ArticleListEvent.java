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
import org.apache.gravitino.listener.api.event.ListEvent;
import org.apache.gravitino.listener.api.event.OperationType;

/** A page of Articles or Article bindings returned to the caller. */
@DeveloperApi
public final class ArticleListEvent extends ArticleEvent implements ListEvent {
  private final int resultCount;

  /**
   * Creates a completed list event.
   *
   * @param user the initiating user
   * @param identifier the metalake or Article identifier
   * @param operation the list operation
   * @param resultCount the number of returned entries
   */
  public ArticleListEvent(
      String user, NameIdentifier identifier, OperationType operation, int resultCount) {
    super(user, identifier, operation);
    this.resultCount = resultCount;
  }

  @Override
  public int resultCount() {
    return resultCount;
  }
}
