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
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.responses.LoadViewResponse;

/** Represent an event after creating Iceberg view successfully. */
@DeveloperApi
public class IcebergCreateViewEvent extends IcebergViewEvent {

  private final CreateViewRequest createViewRequest;
  private final LoadViewResponse loadViewResponse;

  public IcebergCreateViewEvent(
      IcebergRequestContext icebergRequestContext,
      NameIdentifier viewIdentifier,
      CreateViewRequest createViewRequest,
      LoadViewResponse loadViewResponse) {
    super(icebergRequestContext, viewIdentifier);
    this.createViewRequest =
        IcebergRestUtils.cloneIcebergRESTObject(createViewRequest, CreateViewRequest.class);
    this.loadViewResponse =
        IcebergRestUtils.cloneIcebergRESTObject(loadViewResponse, LoadViewResponse.class);
  }

  public CreateViewRequest createViewRequest() {
    return createViewRequest;
  }

  public LoadViewResponse loadViewResponse() {
    return loadViewResponse;
  }
}
