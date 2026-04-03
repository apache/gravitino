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
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.IcebergCommitTransactionEvent;
import org.apache.gravitino.listener.api.event.IcebergCommitTransactionFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCommitTransactionPreEvent;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;

/**
 * {@code IcebergTransactionEventDispatcher} is a decorator for {@link
 * IcebergTransactionOperationDispatcher} that delegates transaction operations to the underlying
 * dispatcher and dispatches corresponding events to an {@link EventBus}.
 */
public class IcebergTransactionEventDispatcher implements IcebergTransactionOperationDispatcher {

  private final IcebergTransactionOperationDispatcher dispatcher;
  private final EventBus eventBus;
  private final String metalakeName;

  public IcebergTransactionEventDispatcher(
      IcebergTransactionOperationDispatcher dispatcher, EventBus eventBus, String metalakeName) {
    this.dispatcher = dispatcher;
    this.eventBus = eventBus;
    this.metalakeName = metalakeName;
  }

  @Override
  public void commitTransaction(
      IcebergRequestContext context, CommitTransactionRequest commitTransactionRequest) {
    NameIdentifier catalogIdentifier = NameIdentifier.of(metalakeName, context.catalogName());
    eventBus.dispatchEvent(
        new IcebergCommitTransactionPreEvent(context, catalogIdentifier, commitTransactionRequest));
    try {
      dispatcher.commitTransaction(context, commitTransactionRequest);
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new IcebergCommitTransactionFailureEvent(
              context, catalogIdentifier, commitTransactionRequest, e));
      throw e;
    }
    eventBus.dispatchEvent(
        new IcebergCommitTransactionEvent(context, catalogIdentifier, commitTransactionRequest));
  }
}
