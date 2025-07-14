/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.info.OwnerInfo;

/** Base class for owner-related failure events in the listener API. */
@DeveloperApi
public abstract class OwnerFailureEvent extends FailureEvent {
  private final OwnerInfo ownerInfo;
  private final MetadataObject.Type type;

  public OwnerFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      OwnerInfo ownerInfo,
      MetadataObject.Type type) {
    super(user, identifier, exception);
    this.ownerInfo = ownerInfo;
    this.type = type;
  }

  public OwnerInfo ownerInfo() {
    return ownerInfo;
  }

  public MetadataObject.Type metadataObjectType() {
    return type;
  }
}
