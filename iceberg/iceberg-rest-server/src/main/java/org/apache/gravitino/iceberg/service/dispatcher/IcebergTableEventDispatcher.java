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

package org.apache.gravitino.iceberg.service.dispatcher;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.IcebergCreateTableEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateTablePreEvent;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/**
 * {@code IcebergTableEventDispatcher} is a decorator for {@link IcebergTableOperationExecutor} that
 * not only delegates table operations to the underlying dispatcher but also dispatches
 * corresponding events to an {@link org.apache.gravitino.listener.EventBus}.
 */
public class IcebergTableEventDispatcher implements IcebergTableOperationDispatcher {

  private IcebergTableOperationDispatcher icebergTableOperationDispatcher;
  private EventBus eventBus;
  private String metalakeName;

  public IcebergTableEventDispatcher(
      IcebergTableOperationDispatcher icebergTableOperationDispatcher,
      EventBus eventBus,
      String metalakeName) {
    this.icebergTableOperationDispatcher = icebergTableOperationDispatcher;
    this.eventBus = eventBus;
    this.metalakeName = metalakeName;
  }

  @Override
  public LoadTableResponse createTable(
      String catalogName, Namespace namespace, CreateTableRequest createTableRequest) {
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, createTableRequest.name());
    NameIdentifier nameIdentifier =
        IcebergRestUtils.getGravitinoNameIdentifier(metalakeName, catalogName, tableIdentifier);
    eventBus.dispatchEvent(
        new IcebergCreateTablePreEvent(
            PrincipalUtils.getCurrentUserName(), nameIdentifier, createTableRequest));
    LoadTableResponse loadTableResponse;
    try {
      loadTableResponse =
          icebergTableOperationDispatcher.createTable(catalogName, namespace, createTableRequest);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergCreateTableFailureEvent(
              PrincipalUtils.getCurrentUserName(), nameIdentifier, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergCreateTableEvent(
            PrincipalUtils.getCurrentUserName(),
            nameIdentifier,
            createTableRequest,
            loadTableResponse));
    return loadTableResponse;
  }
}
