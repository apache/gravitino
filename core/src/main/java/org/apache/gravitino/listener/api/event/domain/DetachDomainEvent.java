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
package org.apache.gravitino.listener.api.event.domain;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.info.DomainInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** The success event for DetachDomain. */
@DeveloperApi
public final class DetachDomainEvent extends DomainEvent {
  private final DomainInfo domainInfo;

  /**
   * Creates the event.
   *
   * @param user The initiating user.
   * @param metalake The metalake name.
   * @param name The domain name addressed by the operation.
   * @param domainInfo The requested or returned domain snapshot.
   */
  public DetachDomainEvent(String user, String metalake, String name, DomainInfo domainInfo) {
    super(user, NameIdentifierUtil.ofDomain(metalake, name));
    this.domainInfo = domainInfo;
  }

  /**
   * @return The requested or returned domain snapshot.
   */
  public DomainInfo domainInfo() {
    return domainInfo;
  }

  /** {@inheritDoc} */
  @Override
  public OperationType operationType() {
    return OperationType.DETACH_DOMAIN;
  }
}
