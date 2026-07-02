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
package org.apache.gravitino.spark.connector.iceberg;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IcebergRestCatalogRegistrar}. */
public class TestIcebergRestCatalogRegistrar {

  private static final String CATALOG_NAME = "iceberg_prod";
  private static final String GRAVITINO_URI = "http://gravitino.example.com:8090";

  // ── URI resolution ─────────────────────────────────────────────────────────

  @Test
  void testExplicitRestUriIsUsedAsIs() {
    String explicit = "http://my-rest-server:9001/iceberg/";
    SparkConf conf = baseConf().set(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI, explicit);
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);
    Assertions.assertEquals(explicit, registrar.getResolvedRestUri());
  }

  @Test
  void testRestUriInferredFromGravitinoUri() {
    SparkConf conf = baseConf();
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);
    Assertions.assertEquals(
        "http://gravitino.example.com:9001/iceberg/", registrar.getResolvedRestUri());
  }

  @Test
  void testRestUriInferredFromHttpsGravitinoUri() {
    // HTTPS gravitino URI must infer port 9433 (Gravitino default HTTPS port for Iceberg REST),
    // not the HTTP default 9001.
    SparkConf conf =
        new SparkConf()
            .set(GravitinoSparkConfig.GRAVITINO_URI, "https://gravitino.example.com:8443")
            .set(GravitinoSparkConfig.GRAVITINO_METALAKE, "test");
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);
    Assertions.assertEquals(
        "https://gravitino.example.com:9433/iceberg/", registrar.getResolvedRestUri());
  }

  // ── Base catalog properties ────────────────────────────────────────────────

  @Test
  void testBaseSparkPropertiesAreSet() {
    SparkConf conf = baseConf();
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);
    Catalog catalog = emptyCatalog();

    registrar.registerCatalog(conf, CATALOG_NAME, catalog);

    String key = "spark.sql.catalog." + CATALOG_NAME;
    Assertions.assertEquals("org.apache.iceberg.spark.SparkCatalog", conf.get(key));
    Assertions.assertEquals("rest", conf.get(key + ".type"));
    Assertions.assertEquals("http://gravitino.example.com:9001/iceberg/", conf.get(key + ".uri"));
    Assertions.assertEquals(CATALOG_NAME, conf.get(key + ".warehouse"));
  }

  @Test
  void testDuplicateRegistrationThrows() {
    SparkConf conf = baseConf();
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);
    Catalog catalog = emptyCatalog();

    registrar.registerCatalog(conf, CATALOG_NAME, catalog);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> registrar.registerCatalog(conf, CATALOG_NAME, catalog));
  }

  // ── Vended-credentials path ────────────────────────────────────────────────

  @Test
  void testVendedCredentialsDelegationHeaderIsSet() {
    SparkConf conf = baseConf();
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);
    Catalog catalog =
        catalogWithProperties(ImmutableMap.of(IcebergConstants.DATA_ACCESS, "vended-credentials"));

    registrar.registerCatalog(conf, CATALOG_NAME, catalog);

    String header =
        conf.get("spark.sql.catalog." + CATALOG_NAME + ".header.X-Iceberg-Access-Delegation");
    Assertions.assertEquals("vended-credentials", header);
  }

  @Test
  void testVendedCredentialsNoS3KeysAreSet() {
    SparkConf conf = baseConf();
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);
    Catalog catalog =
        catalogWithProperties(ImmutableMap.of(IcebergConstants.DATA_ACCESS, "vended-credentials"));

    registrar.registerCatalog(conf, CATALOG_NAME, catalog);

    String prefix = "spark.sql.catalog." + CATALOG_NAME + ".s3.";
    Assertions.assertFalse(
        conf.contains(prefix + "access-key-id"), "No S3 access-key-id should be set");
    Assertions.assertFalse(
        conf.contains(prefix + "secret-access-key"), "No S3 secret-access-key should be set");
  }

  // ── Non-vended (static credential) path ──────────────────────────────────

  @Test
  void testStaticS3CredentialsArePrefixedAndSet() {
    SparkConf conf = baseConf();
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);

    S3SecretKeyCredential s3Cred = new S3SecretKeyCredential("AKID", "SECRET");
    SupportsCredentials sc = mock(SupportsCredentials.class);
    when(sc.getCredentials()).thenReturn(new org.apache.gravitino.credential.Credential[] {s3Cred});

    Catalog catalog = mock(Catalog.class);
    when(catalog.properties()).thenReturn(ImmutableMap.of());
    when(catalog.supportsCredentials()).thenReturn(sc);

    registrar.registerCatalog(conf, CATALOG_NAME, catalog);

    String prefix = "spark.sql.catalog." + CATALOG_NAME + ".";
    Assertions.assertEquals("AKID", conf.get(prefix + "s3.access-key-id"));
    Assertions.assertEquals("SECRET", conf.get(prefix + "s3.secret-access-key"));
  }

  @Test
  void testStaticS3StoragePropertiesArePrefixedAndSet() {
    SparkConf conf = baseConf();
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);

    Map<String, String> catalogProps =
        ImmutableMap.of(
            "s3-endpoint", "http://minio:9000",
            "s3-region", "us-east-1",
            "s3-path-style-access", "true");
    Catalog catalog = catalogWithProperties(catalogProps);

    registrar.registerCatalog(conf, CATALOG_NAME, catalog);

    String prefix = "spark.sql.catalog." + CATALOG_NAME + ".";
    Assertions.assertEquals("http://minio:9000", conf.get(prefix + "s3.endpoint"));
    Assertions.assertEquals("us-east-1", conf.get(prefix + "client.region"));
    Assertions.assertEquals("true", conf.get(prefix + "s3.path-style-access"));
  }

  @Test
  void testNoDelegationHeaderWhenNotVended() {
    SparkConf conf = baseConf();
    IcebergRestCatalogRegistrar registrar = new IcebergRestCatalogRegistrar(conf);
    Catalog catalog = emptyCatalog();

    registrar.registerCatalog(conf, CATALOG_NAME, catalog);

    Assertions.assertFalse(
        conf.contains("spark.sql.catalog." + CATALOG_NAME + ".header.X-Iceberg-Access-Delegation"),
        "Delegation header must not be set when data-access is not vended-credentials");
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private static SparkConf baseConf() {
    return new SparkConf()
        .set(GravitinoSparkConfig.GRAVITINO_URI, GRAVITINO_URI)
        .set(GravitinoSparkConfig.GRAVITINO_METALAKE, "test");
  }

  private static Catalog emptyCatalog() {
    return catalogWithProperties(ImmutableMap.of());
  }

  private static Catalog catalogWithProperties(Map<String, String> properties) {
    Catalog catalog = mock(Catalog.class);
    when(catalog.properties()).thenReturn(properties);
    when(catalog.supportsCredentials())
        .thenThrow(new UnsupportedOperationException("no credential vending"));
    return catalog;
  }
}
