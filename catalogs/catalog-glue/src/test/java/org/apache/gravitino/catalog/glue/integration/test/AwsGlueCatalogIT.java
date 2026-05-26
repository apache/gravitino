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
package org.apache.gravitino.catalog.glue.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs {@link AbstractGlueCatalogIT} scenarios against a real AWS Glue endpoint.
 *
 * <p>This test is <b>skipped by default</b>. Required environment variables:
 *
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}
 *   <li>{@code AWS_SECRET_ACCESS_KEY}
 *   <li>{@code AWS_S3_TEST_BUCKET}
 *   <li>{@code AWS_DEFAULT_REGION} (optional, defaults to {@code us-east-1})
 *   <li>{@code GLUE_CATALOG_ID} (optional, 12-digit AWS account ID)
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwsGlueCatalogIT extends AbstractGlueCatalogIT {

  private static final Logger LOG = LoggerFactory.getLogger(AwsGlueCatalogIT.class);

  @BeforeAll
  void checkAwsEnvironment() {
    Preconditions.checkState(
        System.getenv("AWS_SECRET_ACCESS_KEY") != null, "AWS_SECRET_ACCESS_KEY must be set");
    Preconditions.checkState(
        System.getenv("AWS_S3_TEST_BUCKET") != null, "AWS_S3_TEST_BUCKET must be set");
    if (System.getenv("AWS_DEFAULT_REGION") == null) {
      LOG.info("AWS_DEFAULT_REGION is not set, using default us-east-1");
    }
  }

  @Override
  protected Map<String, String> catalogConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(
        GlueConstants.AWS_REGION, System.getenv().getOrDefault("AWS_DEFAULT_REGION", "us-east-1"));
    config.put(GlueConstants.AWS_ACCESS_KEY_ID, System.getenv("AWS_ACCESS_KEY_ID"));
    config.put(GlueConstants.AWS_SECRET_ACCESS_KEY, System.getenv("AWS_SECRET_ACCESS_KEY"));
    String catalogId = System.getenv("GLUE_CATALOG_ID");
    if (catalogId != null) {
      config.put(GlueConstants.AWS_GLUE_CATALOG_ID, catalogId);
    }
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    if (bucket != null) {
      config.put(GlueConstants.WAREHOUSE, "s3://" + bucket + "/warehouse");
    }
    return config;
  }

  @Test
  void testAlterIcebergMetadata() {
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    String schema = "glue_it_" + System.nanoTime();
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());

    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.LOCATION, "s3://" + bucket + "/" + schema + "/");
    ops.createTable(
        NameIdentifier.of("ml", "cat", schema, "iceberg_alter"),
        new Column[] {Column.of("id", Types.LongType.get(), null)},
        null,
        props,
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    try {
      ops.alterTable(
          NameIdentifier.of("ml", "cat", schema, "iceberg_alter"),
          TableChange.setProperty("comment", "updated comment"));

      Table loaded = ops.loadTable(NameIdentifier.of("ml", "cat", schema, "iceberg_alter"));
      assertEquals("updated comment", loaded.properties().get("comment"));
    } finally {
      ops.dropTable(NameIdentifier.of("ml", "cat", schema, "iceberg_alter"));
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), false);
    }
  }
}
