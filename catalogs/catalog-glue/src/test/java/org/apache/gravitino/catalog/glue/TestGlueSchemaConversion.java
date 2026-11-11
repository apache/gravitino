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

import java.time.Instant;
import java.util.Map;
import software.amazon.awssdk.services.glue.model.Database;

/**
 * Runs {@link GlueSchemaTestBase} scenarios using AWS SDK builders to create {@link Database}
 * objects directly — no network or AWS credentials required.
 *
 * <p>This verifies that the {@link GlueSchema#fromGlueDatabase} conversion logic works correctly
 * for typical Glue API response shapes.
 */
class TestGlueSchemaConversion extends GlueSchemaTestBase {

  @Override
  protected Database provideDatabase(String name, String description, Map<String, String> params) {
    return Database.builder()
        .name(name)
        .description(description)
        .parameters(params)
        .createTime(Instant.now())
        .build();
  }
}
