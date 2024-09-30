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

package org.apache.gravitino.audit;

import java.util.Objects;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.FailureEvent;

/** The default implementation of the audit log. */
public class DefaultFormatter implements Formatter {

  @Override
  public DefaultAuditLog format(Event event) {
    boolean successful = !(event instanceof FailureEvent);
    return DefaultAuditLog.builder()
        .user(event.user())
        .operation(AuditLog.Operation.fromEvent(event))
        .identifier(
            event.identifier() != null
                ? Objects.requireNonNull(event.identifier()).toString()
                : null)
        .timestamp(event.eventTime())
        .successful(successful)
        .build();
  }
}
