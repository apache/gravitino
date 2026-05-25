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

package org.apache.gravitino.listener.api.event;

import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.utils.RequestContext;

/**
 * Represents a post event for Gravitino server operations.
 *
 * <p>The client remote address is captured from {@link RequestContext} at construction time on the
 * servlet thread, so async listener threads can safely call {@link #remoteAddress()} without
 * accessing thread-local storage.
 */
@DeveloperApi
public abstract class Event extends BaseEvent {

  private final String remoteAddress;

  protected Event(String user, NameIdentifier identifier) {
    super(user, identifier);
    String addr = RequestContext.getRemoteAddress();
    this.remoteAddress = StringUtils.isNoneBlank(addr) ? addr : "unknown";
  }

  /**
   * Returns the client remote address captured at event construction time.
   *
   * @return the client IP address, or {@code "unknown"} if the request context was not set.
   */
  @Override
  public String remoteAddress() {
    return remoteAddress;
  }
}
