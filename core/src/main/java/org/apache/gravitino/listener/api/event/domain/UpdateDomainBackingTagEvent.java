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

import javax.annotation.Nullable;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.info.DomainInfo;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** The success event for UpdateDomainBackingTag. */
@DeveloperApi
public final class UpdateDomainBackingTagEvent extends DomainEvent {
  private final @Nullable TagValue backingTag;
  private final DomainInfo domainInfo;

  /**
   * Creates the event.
   *
   * @param user The initiating user.
   * @param metalake The metalake name.
   * @param name The domain name addressed by the operation.
   * @param backingTag The requested backing tag, or null to remove it.
   * @param domainInfo The requested or returned domain snapshot.
   */
  public UpdateDomainBackingTagEvent(
      String user,
      String metalake,
      String name,
      @Nullable TagValue backingTag,
      DomainInfo domainInfo) {
    super(user, NameIdentifierUtil.ofDomain(metalake, name));
    this.backingTag = backingTag;
    this.domainInfo = domainInfo;
  }

  /**
   * @return The requested backing tag, or null to remove it.
   */
  public @Nullable TagValue backingTag() {
    return backingTag;
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
    return OperationType.UPDATE_DOMAIN_BACKING_TAG;
  }
}
