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

import lombok.Builder;
import lombok.SneakyThrows;
import org.apache.gravitino.json.JsonUtils;

/** The default implementation of the audit log. */
@Builder
public class DefaultAuditLog implements AuditLog {

  private String user;

  private Operation operation;

  private String identifier;

  private long timestamp;

  private boolean successful;

  @Override
  public String user() {
    return user;
  }

  @Override
  public Operation operation() {
    return operation;
  }

  @Override
  public String identifier() {
    return identifier;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public boolean successful() {
    return successful;
  }

  @SneakyThrows
  public String toString() {
    return JsonUtils.anyFieldMapper().writeValueAsString(this);
  }
}
