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

import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;

/**
 * {@code IcebergTransactionHookDispatcher} is a decorator for {@link
 * IcebergTransactionOperationDispatcher} that runs post-operation hooks after delegating to the
 * underlying dispatcher.
 */
public class IcebergTransactionHookDispatcher implements IcebergTransactionOperationDispatcher {

  private final IcebergTransactionOperationDispatcher dispatcher;

  public IcebergTransactionHookDispatcher(IcebergTransactionOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void commitTransaction(
      IcebergRequestContext context, CommitTransactionRequest commitTransactionRequest) {
    dispatcher.commitTransaction(context, commitTransactionRequest);
  }
}
