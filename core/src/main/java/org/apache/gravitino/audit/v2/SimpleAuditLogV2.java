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

package org.apache.gravitino.audit.v2;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.AuditLog;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;

/**
 * Compared to {@link org.apache.gravitino.audit.SimpleAuditLog}, adds audit log for Iceberg REST
 * server, add eventSource and remoteAddress to audit log.
 */
public class SimpleAuditLogV2 implements AuditLog {

  private final BaseEvent event;

  public SimpleAuditLogV2(BaseEvent event) {
    this.event = event;
  }

  @Override
  public String user() {
    return event.user();
  }

  @Override
  @SuppressWarnings("deprecation")
  public Operation operation() {
    return CompatibilityUtils.toAuditLogOperation(event.operationType());
  }

  @Override
  public String identifier() {
    return Optional.ofNullable(event.identifier()).map(NameIdentifier::toString).orElse(null);
  }

  @Override
  public long timestamp() {
    return event.eventTime();
  }

  @Override
  @SuppressWarnings("deprecation")
  public Status status() {
    return CompatibilityUtils.toAuditLogStatus(event.operationStatus());
  }

  @Override
  public String remoteAddress() {
    return event.remoteAddress();
  }

  @Override
  public OperationStatus operationStatus() {
    return event.operationStatus();
  }

  @Override
  public OperationType operationType() {
    return event.operationType();
  }

  @Override
  public EventSource eventSource() {
    return event.eventSource();
  }

  @Override
  public Map<String, String> customInfo() {
    return event.customInfo();
  }

  @Override
  public String toString() {
    return String.format(
        "[%s]\t%s\t%s\t%s\t%s\t%s\t%s",
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp()),
        user(),
        operationType(),
        identifier(),
        operationStatus(),
        eventSource(),
        remoteAddress());
  }
}
