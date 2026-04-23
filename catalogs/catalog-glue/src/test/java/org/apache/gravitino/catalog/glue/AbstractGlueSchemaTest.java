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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Database;

/**
 * Abstract base for {@link GlueSchema} conversion tests.
 *
 * <p>Subclasses provide a {@link Database} object however they like (SDK builder, real AWS API,
 * etc.). The test scenarios are defined once here and shared across all implementations.
 */
abstract class AbstractGlueSchemaTest {

  /**
   * Returns a Glue {@link Database} with the given fields. Subclasses may create this via the SDK
   * builder (synthetic) or by calling the real Glue API and retrieving the result.
   */
  protected abstract Database provideDatabase(
      String name, String description, Map<String, String> params);

  /** Clean up after each test (e.g. delete real Glue databases). Default: no-op. */
  protected void cleanup(String name) {}

  // -------------------------------------------------------------------------
  // Test scenarios
  // -------------------------------------------------------------------------

  @Test
  void testAllFieldsMapped() {
    String dbName = uniqueName("test_all_fields");
    Map<String, String> params = Map.of("owner", "alice", "env", "prod");
    Database db = provideDatabase(dbName, "a test database", params);
    try {
      GlueSchema schema = GlueSchema.fromGlueDatabase(db);
      assertEquals(dbName, schema.name());
      assertEquals("a test database", schema.comment());
      assertEquals("alice", schema.properties().get("owner"));
      assertEquals("prod", schema.properties().get("env"));
      assertNotNull(schema.auditInfo());
    } finally {
      cleanup(dbName);
    }
  }

  @Test
  void testNullDescription() {
    String dbName = uniqueName("test_null_desc");
    Database db = provideDatabase(dbName, null, Collections.emptyMap());
    try {
      GlueSchema schema = GlueSchema.fromGlueDatabase(db);
      assertNull(schema.comment());
    } finally {
      cleanup(dbName);
    }
  }

  @Test
  void testEmptyParameters() {
    String dbName = uniqueName("test_empty_params");
    Database db = provideDatabase(dbName, "desc", Collections.emptyMap());
    try {
      GlueSchema schema = GlueSchema.fromGlueDatabase(db);
      assertNotNull(schema.properties());
      assertTrue(schema.properties().isEmpty());
    } finally {
      cleanup(dbName);
    }
  }

  @Test
  void testCreateTimeInAuditInfo() {
    String dbName = uniqueName("test_audit");
    Database db = provideDatabase(dbName, null, Collections.emptyMap());
    try {
      GlueSchema schema = GlueSchema.fromGlueDatabase(db);
      // Glue always sets createTime; audit info must reflect it
      assertNotNull(schema.auditInfo().createTime());
    } finally {
      cleanup(dbName);
    }
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Returns a name unique enough to avoid collisions across parallel test runs. */
  protected String uniqueName(String base) {
    return base + "_" + System.currentTimeMillis();
  }
}
