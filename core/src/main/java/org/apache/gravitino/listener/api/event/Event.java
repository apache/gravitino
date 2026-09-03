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

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
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
 *
 * <p>The current request's query parameters are captured the same way, raw (not redacted — see
 * {@code org.apache.gravitino.audit.AuditLogRedactor}), and merged into {@link #customInfo()}
 * automatically. This gives every event in the system — including ones with no subclass-specific
 * {@code customInfo} — automatic audit coverage of the parameters that produced it, with no
 * per-event-class wiring required.
 *
 * <p>{@link #customInfo()} itself is {@code final}: a subclass that wants to contribute its own
 * facts must override {@link #ownCustomInfo()} instead, never {@code customInfo()} directly. This
 * is deliberate — an earlier version of this class let subclasses override {@code customInfo()}
 * directly, which let several of them (accidentally) discard the automatically captured query
 * parameters instead of merging with them. Sealing the merge here makes that class of bug
 * impossible to reintroduce.
 */
@DeveloperApi
public abstract class Event extends BaseEvent {

  private final String remoteAddress;
  private final Map<String, String> autoCustomInfo;

  protected Event(String user, NameIdentifier identifier) {
    super(user, identifier);
    String addr = RequestContext.getRemoteAddress();
    this.remoteAddress = StringUtils.isNoneBlank(addr) ? addr : "unknown";
    this.autoCustomInfo = RequestContext.getRequestQueryParams();
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

  /**
   * Returns the current request's (raw) query parameters merged with this event's own facts (from
   * {@link #ownCustomInfo()}); the subclass's own values win on a key collision. This method is
   * {@code final} — override {@link #ownCustomInfo()} to contribute subclass-specific facts.
   *
   * @return the merged custom info, or just the automatically captured query parameters if the
   *     subclass contributes nothing of its own.
   */
  @Override
  public final Map<String, String> customInfo() {
    Map<String, String> own = ownCustomInfo();
    if (own.isEmpty()) {
      return autoCustomInfo;
    }
    Map<String, String> merged = new LinkedHashMap<>(autoCustomInfo);
    merged.putAll(own);
    return ImmutableMap.copyOf(merged);
  }

  /**
   * Returns this event's own explicit facts, to be merged with the automatically captured request
   * query parameters by {@link #customInfo()}. Override this — not {@code customInfo()} — to
   * contribute subclass-specific audit facts.
   *
   * @return this event's own facts, or an empty map if it has none of its own.
   */
  protected Map<String, String> ownCustomInfo() {
    return ImmutableMap.of();
  }
}
