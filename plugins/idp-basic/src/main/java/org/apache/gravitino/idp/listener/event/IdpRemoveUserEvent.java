/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.idp.listener.event;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.idp.listener.IdpNameIdentifierUtil;
import org.apache.gravitino.listener.api.event.OperationType;

/** Success event for removing a built-in IdP user. */
@DeveloperApi
public class IdpRemoveUserEvent extends IdpUserEvent {
  private final String removedUserName;
  private final boolean isExists;

  public IdpRemoveUserEvent(String initiator, String removedUserName, boolean isExists) {
    super(initiator, IdpNameIdentifierUtil.ofUser(removedUserName));
    this.removedUserName = removedUserName;
    this.isExists = isExists;
  }

  public String removedUserName() {
    return removedUserName;
  }

  public boolean isExists() {
    return isExists;
  }

  @Override
  public OperationType operationType() {
    return OperationType.REMOVE_USER;
  }
}
