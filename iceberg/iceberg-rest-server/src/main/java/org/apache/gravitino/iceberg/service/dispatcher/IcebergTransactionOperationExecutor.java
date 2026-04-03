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

import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes {@link IcebergTransactionOperationDispatcher} operations against an Iceberg catalog. */
public class IcebergTransactionOperationExecutor implements IcebergTransactionOperationDispatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergTransactionOperationExecutor.class);

  private final IcebergCatalogWrapperManager icebergCatalogWrapperManager;

  public IcebergTransactionOperationExecutor(
      IcebergCatalogWrapperManager icebergCatalogWrapperManager) {
    this.icebergCatalogWrapperManager = icebergCatalogWrapperManager;
  }

  @Override
  public void commitTransaction(
      IcebergRequestContext context, CommitTransactionRequest commitTransactionRequest) {
    LOG.info(
        "Committing transaction for catalog: {}, tableChanges count: {}",
        context.catalogName(),
        commitTransactionRequest.tableChanges().size());
    icebergCatalogWrapperManager
        .getCatalogWrapper(context.catalogName())
        .commitTransaction(commitTransactionRequest);
  }
}
