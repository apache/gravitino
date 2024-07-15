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
package org.apache.gravitino.trino.connector.metadata;

import java.util.Map;
import org.apache.gravitino.Schema;

/** Help Apache Gravitino connector access SchemaMetadata from Gravitino client. */
public class GravitinoSchema {

  private final String schemaName;
  private final Map<String, String> properties;
  private final String comment;

  public GravitinoSchema(Schema schema) {
    this.schemaName = schema.name();
    this.properties = schema.properties();
    this.comment = schema.comment();
  }

  public GravitinoSchema(String schemaName, Map<String, String> properties, String comment) {
    this.schemaName = schemaName;
    this.properties = properties;
    this.comment = comment;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public String getName() {
    return schemaName;
  }

  public String getComment() {
    return comment;
  }
}
