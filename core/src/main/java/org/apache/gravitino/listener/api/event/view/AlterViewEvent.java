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

package org.apache.gravitino.listener.api.event.view;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.info.ViewInfo;
import org.apache.gravitino.rel.ViewChange;

/** Successful alter-view event. */
@DeveloperApi
public final class AlterViewEvent extends ViewEvent {
  private final ViewInfo updatedViewInfo;
  private final ViewChange[] viewChanges;

  public AlterViewEvent(
      String user, NameIdentifier identifier, ViewChange[] viewChanges, ViewInfo updatedViewInfo) {
    super(user, identifier);
    this.viewChanges = viewChanges.clone();
    this.updatedViewInfo = updatedViewInfo;
  }

  public ViewInfo updatedViewInfo() {
    return updatedViewInfo;
  }

  public ViewChange[] viewChanges() {
    return viewChanges;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ALTER_VIEW;
  }
}
