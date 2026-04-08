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
package org.apache.gravitino.catalog.glue;

import java.util.Map;
import org.apache.gravitino.Schema;
import org.apache.gravitino.meta.AuditInfo;

public class GlueSchema implements Schema {

  private final String name;
  private final String comment;
  private final Map<String, String> properties;
  private final AuditInfo auditInfo;

  public GlueSchema(
      String name, String comment, Map<String, String> properties, AuditInfo auditInfo) {
    this.name = name;
    this.comment = comment;
    this.properties = properties;
    this.auditInfo = auditInfo;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }
}