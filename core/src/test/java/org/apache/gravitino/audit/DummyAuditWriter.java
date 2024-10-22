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

import java.util.LinkedList;
import java.util.Map;
import lombok.Getter;

public class DummyAuditWriter implements AuditLogWriter {
  private Formatter formatter;

  @Getter private final LinkedList<DummyAuditLog> auditLogs = new LinkedList<>();

  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  @Override
  public void init(Formatter formatter, Map<String, String> properties) {
    this.formatter = formatter;
  }

  @Override
  public void doWrite(AuditLog auditLog) {
    auditLogs.add((DummyAuditLog) auditLog);
  }

  @Override
  public void close() {}

  @Override
  public String name() {
    return "dummy";
  }
}
