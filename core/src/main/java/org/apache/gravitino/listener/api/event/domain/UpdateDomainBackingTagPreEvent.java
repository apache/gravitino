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
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** The pre event for UpdateDomainBackingTag. */
@DeveloperApi
public final class UpdateDomainBackingTagPreEvent extends DomainPreEvent {
  private final @Nullable TagValue backingTag;

  /**
   * Creates the event.
   *
   * @param user The initiating user.
   * @param metalake The metalake name.
   * @param name The domain name addressed by the operation.
   * @param backingTag The requested backing tag, or null to remove it.
   */
  public UpdateDomainBackingTagPreEvent(
      String user, String metalake, String name, @Nullable TagValue backingTag) {
    super(user, NameIdentifierUtil.ofDomain(metalake, name));
    this.backingTag = backingTag;
  }

  /**
   * @return The requested backing tag, or null to remove it.
   */
  public @Nullable TagValue backingTag() {
    return backingTag;
  }

  /** {@inheritDoc} */
  @Override
  public OperationType operationType() {
    return OperationType.UPDATE_DOMAIN_BACKING_TAG;
  }
}
