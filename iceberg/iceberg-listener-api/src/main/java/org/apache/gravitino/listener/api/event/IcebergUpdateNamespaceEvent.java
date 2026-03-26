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

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represent an event after updating Iceberg namespace successfully. */
@DeveloperApi
public class IcebergUpdateNamespaceEvent extends IcebergNamespaceEvent {

  private final Map<String, Object> updateNamespacePropertiesRequest;
  private final Map<String, Object> updateNamespacePropertiesResponse;

  public IcebergUpdateNamespaceEvent(
      IcebergRequestContext icebergRequestContext,
      NameIdentifier nameIdentifier,
      Object updateNamespacePropertiesRequest,
      Object updateNamespacePropertiesResponse) {
    super(icebergRequestContext, nameIdentifier);
    this.updateNamespacePropertiesRequest =
        IcebergEventUtils.toSerializableMap(updateNamespacePropertiesRequest);
    this.updateNamespacePropertiesResponse =
        IcebergEventUtils.toSerializableMap(updateNamespacePropertiesResponse);
  }

  public Map<String, Object> updateNamespacePropertiesRequest() {
    return updateNamespacePropertiesRequest;
  }

  public Map<String, Object> updateNamespacePropertiesResponse() {
    return updateNamespacePropertiesResponse;
  }

  @Override
  public OperationType operationType() {
    return OperationType.ALTER_SCHEMA;
  }
}
