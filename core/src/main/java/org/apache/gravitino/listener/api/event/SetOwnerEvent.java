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

/**
 * Represents an event that occurs when an owner is set for a metadata object. This event is
 * triggered after the owner has been successfully set.
 */
@DeveloperApi
public final class SetOwnerEvent extends OwnerEvent {
  public SetOwnerEvent(
      String user, NameIdentifier identifier, OwnerInfo ownerInfo, MetadataObject.Type type) {
    super(user, identifier, ownerInfo, type);
  }

  @Override
  public OperationType operationType() {
    return OperationType.SET_OWNER;
  }
}
