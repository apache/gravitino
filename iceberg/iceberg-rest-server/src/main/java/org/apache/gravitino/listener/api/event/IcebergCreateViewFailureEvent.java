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

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.iceberg.rest.requests.CreateViewRequest;

/** Represent a failure event when creating Iceberg view failed. */
@DeveloperApi
public class IcebergCreateViewFailureEvent extends IcebergViewFailureEvent {
  private final CreateViewRequest createViewRequest;

  public IcebergCreateViewFailureEvent(
      IcebergRequestContext icebergRequestContext,
      NameIdentifier viewIdentifier,
      CreateViewRequest createViewRequest,
      Exception e) {
    super(icebergRequestContext, viewIdentifier, e);
    this.createViewRequest =
        IcebergRESTUtils.cloneIcebergRESTObject(createViewRequest, CreateViewRequest.class);
  }

  public CreateViewRequest createViewRequest() {
    return createViewRequest;
  }

  @Override
  public OperationType operationType() {
    return OperationType.CREATE_VIEW;
  }
}
