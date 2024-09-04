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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Schema;
import org.junit.jupiter.api.Test;

public class TestGravitinoSchema {

  @Test
  public void testGravitinoSchema() {
    Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "test prop1");

    Schema mockSchema = mockSchema("db1", "test schema", properties);
    GravitinoSchema schema = new GravitinoSchema(mockSchema);

    assertEquals(schema.getName(), mockSchema.name());
    assertEquals(schema.getComment(), mockSchema.comment());
    assertEquals(schema.getProperties(), mockSchema.properties());
  }

  public static Schema mockSchema(String name, String comment, Map<String, String> properties) {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.name()).thenReturn(name);
    when(mockSchema.comment()).thenReturn(comment);
    when(mockSchema.properties()).thenReturn(properties);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockSchema.auditInfo()).thenReturn(mockAudit);

    return mockSchema;
  }
}
