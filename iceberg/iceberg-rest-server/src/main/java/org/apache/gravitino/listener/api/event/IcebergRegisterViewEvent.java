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
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.responses.LoadViewResponse;

/** Represent an event after registering Iceberg view successfully. */
@DeveloperApi
public class IcebergRegisterViewEvent extends IcebergViewEvent {

  private final RegisterViewRequest registerViewRequest;
  private final LoadViewResponse loadViewResponse;

  public IcebergRegisterViewEvent(
      IcebergRequestContext icebergRequestContext,
      NameIdentifier viewIdentifier,
      RegisterViewRequest registerViewRequest,
      LoadViewResponse loadViewResponse) {
    super(icebergRequestContext, viewIdentifier);
    this.registerViewRequest =
        IcebergRESTUtils.cloneIcebergRESTObject(registerViewRequest, RegisterViewRequest.class);
    this.loadViewResponse =
        IcebergRESTUtils.cloneIcebergRESTObject(loadViewResponse, LoadViewResponse.class);
  }

  public RegisterViewRequest registerViewRequest() {
    return registerViewRequest;
  }

  public LoadViewResponse loadViewResponse() {
    return loadViewResponse;
  }

  @Override
  public OperationType operationType() {
    return OperationType.REGISTER_VIEW;
  }
}
