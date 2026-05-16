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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;

/**
 * Successful list-views event. Like {@link org.apache.gravitino.listener.api.event.ListTableEvent},
 * listed identifiers are not stored on the event.
 */
@DeveloperApi
public final class ListViewEvent extends ViewEvent {
  private final Namespace namespace;

  public ListViewEvent(String user, Namespace namespace) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
  }

  public Namespace namespace() {
    return namespace;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_VIEW;
  }
}
