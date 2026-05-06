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

package org.apache.gravitino.spark.connector.glue;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestGluePropertiesConverter {
  private final GluePropertiesConverter converter = GluePropertiesConverter.getInstance();

  // --- toIcebergCatalogProperties tests ---

  @Test
  void testToIcebergCatalogPropertiesWithRegion() {
    Map<String, String> icebergProps =
        converter.toIcebergCatalogProperties(
            ImmutableMap.of(GluePropertiesConverter.AWS_REGION, "us-east-1"));
    Assertions.assertEquals(
        "org.apache.iceberg.aws.glue.GlueCatalog", icebergProps.get("catalog-impl"));
    Assertions.assertEquals("us-east-1", icebergProps.get(GluePropertiesConverter.CLIENT_REGION));
    Assertions.assertEquals(2, icebergProps.size());
  }

  @Test
  void testToIcebergCatalogPropertiesWithAllProperties() {
    Map<String, String> icebergProps =
        converter.toIcebergCatalogProperties(
            ImmutableMap.of(
                GluePropertiesConverter.AWS_REGION, "us-west-2",
                GluePropertiesConverter.AWS_GLUE_CATALOG_ID, "123456789012",
                GluePropertiesConverter.AWS_GLUE_ENDPOINT, "http://localhost:4566",
                GluePropertiesConverter.AWS_ACCESS_KEY_ID, "AKIAIOSFODNN7EXAMPLE",
                GluePropertiesConverter.AWS_SECRET_ACCESS_KEY,
                    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
    Assertions.assertEquals(
        "org.apache.iceberg.aws.glue.GlueCatalog", icebergProps.get("catalog-impl"));
    Assertions.assertEquals("us-west-2", icebergProps.get(GluePropertiesConverter.CLIENT_REGION));
    Assertions.assertEquals("123456789012", icebergProps.get(GluePropertiesConverter.GLUE_ID));
    Assertions.assertEquals(
        "http://localhost:4566", icebergProps.get(GluePropertiesConverter.GLUE_ENDPOINT));
    Assertions.assertEquals(
        "org.apache.iceberg.aws.static.SessionCredentialsProvider",
        icebergProps.get(GluePropertiesConverter.CLIENT_CREDENTIALS_PROVIDER));
    Assertions.assertEquals("AKIAIOSFODNN7EXAMPLE", icebergProps.get("client.access-key-id"));
    Assertions.assertEquals(
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", icebergProps.get("client.secret-key"));
    Assertions.assertEquals(7, icebergProps.size());
  }

  @Test
  void testToIcebergCatalogPropertiesWithOptionalCatalogId() {
    Map<String, String> icebergProps =
        converter.toIcebergCatalogProperties(
            ImmutableMap.of(GluePropertiesConverter.AWS_REGION, "eu-central-1"));
    Assertions.assertEquals(
        "eu-central-1", icebergProps.get(GluePropertiesConverter.CLIENT_REGION));
    Assertions.assertNull(icebergProps.get(GluePropertiesConverter.GLUE_ID));
  }

  @Test
  void testToIcebergCatalogPropertiesWithOptionalEndpoint() {
    Map<String, String> icebergProps =
        converter.toIcebergCatalogProperties(
            ImmutableMap.of(
                GluePropertiesConverter.AWS_REGION,
                "us-east-1",
                GluePropertiesConverter.AWS_GLUE_ENDPOINT,
                "http://localhost:4566"));
    Assertions.assertEquals(
        "http://localhost:4566", icebergProps.get(GluePropertiesConverter.GLUE_ENDPOINT));
  }

  @Test
  void testToIcebergCatalogPropertiesWithStaticCredentials() {
    Map<String, String> icebergProps =
        converter.toIcebergCatalogProperties(
            ImmutableMap.of(
                GluePropertiesConverter.AWS_REGION, "us-east-1",
                GluePropertiesConverter.AWS_ACCESS_KEY_ID, "access-key",
                GluePropertiesConverter.AWS_SECRET_ACCESS_KEY, "secret-key"));
    Assertions.assertEquals(
        "org.apache.iceberg.aws.static.SessionCredentialsProvider",
        icebergProps.get(GluePropertiesConverter.CLIENT_CREDENTIALS_PROVIDER));
    Assertions.assertEquals("access-key", icebergProps.get("client.access-key-id"));
    Assertions.assertEquals("secret-key", icebergProps.get("client.secret-key"));
  }

  @Test
  void testToIcebergCatalogPropertiesMissingRegionThrows() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> converter.toIcebergCatalogProperties(ImmutableMap.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toIcebergCatalogProperties(
                ImmutableMap.of(GluePropertiesConverter.AWS_REGION, "")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toIcebergCatalogProperties(
                ImmutableMap.of(GluePropertiesConverter.AWS_REGION, "  ")));
  }

  @Test
  void testToIcebergCatalogPropertiesNullInputThrows() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> converter.toIcebergCatalogProperties(null));
  }

  // --- toSparkCatalogProperties tests (single-arg version) ---

  @Test
  void testToSparkCatalogPropertiesEmpty() {
    Map<String, String> sparkProps = converter.toSparkCatalogProperties(ImmutableMap.of());
    Assertions.assertTrue(sparkProps.isEmpty());
  }

  @Test
  void testToSparkCatalogPropertiesWithRegion() {
    Map<String, String> sparkProps =
        converter.toSparkCatalogProperties(
            ImmutableMap.of(GluePropertiesConverter.AWS_REGION, "us-east-1"));
    Assertions.assertEquals("us-east-1", sparkProps.get(GluePropertiesConverter.CLIENT_REGION));
    Assertions.assertEquals(1, sparkProps.size());
  }

  @Test
  void testToSparkCatalogPropertiesWithCredentials() {
    Map<String, String> sparkProps =
        converter.toSparkCatalogProperties(
            ImmutableMap.of(
                GluePropertiesConverter.AWS_REGION, "us-east-1",
                GluePropertiesConverter.AWS_ACCESS_KEY_ID, "my-access-key",
                GluePropertiesConverter.AWS_SECRET_ACCESS_KEY, "my-secret-key"));
    Assertions.assertEquals("us-east-1", sparkProps.get(GluePropertiesConverter.CLIENT_REGION));
    Assertions.assertEquals(
        "org.apache.iceberg.aws.static.SessionCredentialsProvider",
        sparkProps.get(GluePropertiesConverter.CLIENT_CREDENTIALS_PROVIDER));
    Assertions.assertEquals("my-access-key", sparkProps.get("client.access-key-id"));
    Assertions.assertEquals("my-secret-key", sparkProps.get("client.secret-key"));
  }

  @Test
  void testToSparkCatalogPropertiesWithOnlyAccessKey() {
    // Only access key without secret key - access key is not propagated (both required),
    // only the region is passed through to the Spark catalog.
    Map<String, String> sparkProps =
        converter.toSparkCatalogProperties(
            ImmutableMap.of(
                GluePropertiesConverter.AWS_REGION,
                "us-east-1",
                GluePropertiesConverter.AWS_ACCESS_KEY_ID,
                "only-access-key"));
    Assertions.assertEquals("us-east-1", sparkProps.get(GluePropertiesConverter.CLIENT_REGION));
    Assertions.assertNull(sparkProps.get(GluePropertiesConverter.CLIENT_CREDENTIALS_PROVIDER));
    Assertions.assertNull(sparkProps.get("client.access-key-id"));
    // Only client.region is set
    Assertions.assertEquals(1, sparkProps.size());
  }

  @Test
  void testToSparkCatalogPropertiesNullInputThrows() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> converter.toSparkCatalogProperties(null));
  }

  // --- toGravitinoTableProperties tests ---

  @Test
  void testToGravitinoTableProperties() {
    Map<String, String> tableProps =
        converter.toGravitinoTableProperties(ImmutableMap.of("key1", "value1", "key2", "value2"));
    Assertions.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), tableProps);
    Assertions.assertNotSame(tableProps, ImmutableMap.of("key1", "value1", "key2", "value2"));
  }

  @Test
  void testToGravitinoTablePropertiesEmpty() {
    Map<String, String> tableProps = converter.toGravitinoTableProperties(ImmutableMap.of());
    Assertions.assertTrue(tableProps.isEmpty());
  }

  // --- toSparkTableProperties tests ---

  @Test
  void testToSparkTableProperties() {
    Map<String, String> sparkTableProps =
        converter.toSparkTableProperties(ImmutableMap.of("key1", "value1", "key2", "value2"));
    Assertions.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), sparkTableProps);
    Assertions.assertNotSame(sparkTableProps, ImmutableMap.of("key1", "value1", "key2", "value2"));
  }

  @Test
  void testToSparkTablePropertiesEmpty() {
    Map<String, String> sparkTableProps = converter.toSparkTableProperties(ImmutableMap.of());
    Assertions.assertTrue(sparkTableProps.isEmpty());
  }
}
