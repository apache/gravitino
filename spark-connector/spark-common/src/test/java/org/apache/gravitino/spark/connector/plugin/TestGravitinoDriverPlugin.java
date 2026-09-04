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

package org.apache.gravitino.spark.connector.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.authorization.GravitinoAuthorizationSparkSessionExtensions;
import org.apache.gravitino.spark.connector.plugin.GravitinoDriverPlugin.DynamicBearerTokenProvider;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestGravitinoDriverPlugin {

  @Test
  void testIcebergExtensionName() {
    Assertions.assertEquals(
        IcebergSparkSessionExtensions.class.getName(),
        GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS);
  }

  @Test
  void testAlwaysRegistersAuthorizationExtension() {
    SparkConf sparkConf = new SparkConf(false);

    new GravitinoDriverPlugin().registerSqlExtensions(sparkConf);

    assertEquals(
        GravitinoAuthorizationSparkSessionExtensions.class.getName(),
        sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testDoesNotDuplicateAuthorizationExtension() {
    SparkConf sparkConf = new SparkConf(false);
    String extension = GravitinoAuthorizationSparkSessionExtensions.class.getName();
    sparkConf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extension);

    new GravitinoDriverPlugin().registerSqlExtensions(sparkConf);

    assertEquals(extension, sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testTokenAuthTypeBuildsClient() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "a-token");

    // The client cannot reach a server here, but it must get past auth configuration first: an
    // unsupported auth type or a missing token would fail before any connection is attempted.
    Exception e =
        Assertions.assertThrows(
            Exception.class,
            () ->
                GravitinoDriverPlugin.createGravitinoClient(
                    "http://127.0.0.1:1", "metalake", sparkConf, "user", ImmutableMap.of()));
    Assertions.assertFalse(e instanceof UnsupportedOperationException, e.toString());
    Assertions.assertFalse(e instanceof IllegalArgumentException, e.toString());
  }

  @Test
  void testTokenProviderReturnsBearerToken() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "a-token");

    assertEquals(
        "Bearer a-token",
        new String(
            new DynamicBearerTokenProvider(sparkConf).getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenIsResolvedOnEveryRequest() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "first-token");
    DynamicBearerTokenProvider provider = new DynamicBearerTokenProvider(sparkConf);

    assertEquals("Bearer first-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));

    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "second-token");

    assertEquals(
        "Bearer second-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenFileTakesPrecedenceAndIsRereadEveryRequest(@TempDir Path tempDir)
      throws IOException {
    Path tokenFile = tempDir.resolve("token");
    Files.write(tokenFile, "file-token\n".getBytes(StandardCharsets.UTF_8));
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "conf-token");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_FILE, tokenFile.toString());
    DynamicBearerTokenProvider provider = new DynamicBearerTokenProvider(sparkConf);

    assertEquals("Bearer file-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));

    Files.write(tokenFile, "rotated-token\n".getBytes(StandardCharsets.UTF_8));

    assertEquals(
        "Bearer rotated-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenAuthTypeWithoutTokenFails() {
    SparkConf sparkConf = tokenAuthConf();

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoDriverPlugin.createGravitinoClient(
                    "http://127.0.0.1:1", "metalake", sparkConf, "user", ImmutableMap.of()));
    Assertions.assertTrue(e.getMessage().contains(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE));
    Assertions.assertTrue(e.getMessage().contains(GravitinoSparkConfig.GRAVITINO_TOKEN_FILE));
  }

  @Test
  void testSkipsCatalogAlreadyConfiguredInSpark() {
    SparkConf sparkConf =
        new SparkConf(false).set("spark.sql.catalog.existing", "example.UserCatalog");
    Catalog catalog = mock(Catalog.class);
    when(catalog.provider()).thenReturn("hive");

    new GravitinoDriverPlugin()
        .registerGravitinoCatalogs(sparkConf, Collections.singletonMap("existing", catalog));

    assertEquals("example.UserCatalog", sparkConf.get("spark.sql.catalog.existing"));
  }

  private static SparkConf tokenAuthConf() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.TOKEN_AUTH_TYPE);
    return sparkConf;
  }
}
