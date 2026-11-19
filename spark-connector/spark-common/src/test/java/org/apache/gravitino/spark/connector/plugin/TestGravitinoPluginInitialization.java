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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies plugin initialization and SQL execution with a real local Spark context. */
@Timeout(60)
public class TestGravitinoPluginInitialization {

  @TempDir Path tempDir;

  @AfterEach
  void clearSparkSessions() {
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {GravitinoSparkConfig.GRAVITINO_URI, GravitinoSparkConfig.GRAVITINO_METALAKE})
  void testSelectOneWithoutARequiredSetting(String missingSetting) throws IOException {
    SparkConf conf = sparkConf();
    conf.remove(missingSetting);

    assertSelectOne(conf);
  }

  @Test
  void testSelectOneWithoutAnyGravitinoConnectionSettings() throws IOException {
    SparkConf conf = sparkConf();
    conf.remove(GravitinoSparkConfig.GRAVITINO_URI);
    conf.remove(GravitinoSparkConfig.GRAVITINO_METALAKE);

    assertSelectOne(conf);
  }

  @Test
  void testSelectOneWhenGravitinoCannotBeReached() throws IOException {
    SparkConf conf = sparkConf();
    conf.set(GravitinoSparkConfig.GRAVITINO_URI, unavailableUri());

    assertSelectOne(conf);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {GravitinoSparkConfig.GRAVITINO_URI, GravitinoSparkConfig.GRAVITINO_METALAKE})
  void testFailFastRejectsARequiredSettingBeforeSparkStarts(String missingSetting) {
    SparkConf conf = sparkConf();
    conf.remove(missingSetting);
    conf.set(GravitinoSparkConfig.GRAVITINO_FAIL_FAST, "true");

    assertStartupFails(conf, IllegalArgumentException.class);
  }

  @Test
  void testFailFastRejectsAnUnreachableServerBeforeSparkStarts() throws IOException {
    SparkConf conf = sparkConf();
    conf.set(GravitinoSparkConfig.GRAVITINO_URI, unavailableUri());
    conf.set(GravitinoSparkConfig.GRAVITINO_FAIL_FAST, "true");

    assertStartupFails(conf, ConnectException.class);
  }

  private SparkConf sparkConf() {
    return new SparkConf(false)
        .setMaster("local[1]")
        .setAppName("test-gravitino-plugin-initialization")
        .set("spark.plugins", GravitinoSparkPlugin.class.getName())
        .set("spark.ui.enabled", "false")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.sql.warehouse.dir", tempDir.resolve("warehouse").toUri().toString())
        .set(GravitinoSparkConfig.GRAVITINO_URI, "http://127.0.0.1:1")
        .set(GravitinoSparkConfig.GRAVITINO_METALAKE, "test")
        .set(GravitinoSparkConfig.GRAVITINO_CLIENT_CONFIG_PREFIX + "connectionTimeoutMs", "200")
        .set(GravitinoSparkConfig.GRAVITINO_CLIENT_CONFIG_PREFIX + "socketTimeoutMs", "200");
  }

  private static void assertSelectOne(SparkConf conf) throws IOException {
    try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {
      assertEquals(1, spark.sql("SELECT 1").collectAsList().get(0).getInt(0));
      assertFalse(spark.sparkContext().isStopped());
      assertFalse(
          spark.sparkContext().conf().contains(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
      assertThrows(IllegalStateException.class, GravitinoCatalogManager::get);
    }
  }

  private static void assertStartupFails(SparkConf conf, Class<? extends Throwable> expectedCause) {
    Exception failure =
        assertThrows(
            Exception.class,
            () -> {
              try (SparkSession spark = SparkSession.builder().config(conf).getOrCreate()) {
                assertFalse(spark.sparkContext().isStopped());
              }
            });
    assertTrue(hasCause(failure, expectedCause), failure::toString);
    assertThrows(IllegalStateException.class, GravitinoCatalogManager::get);
  }

  private static boolean hasCause(Throwable failure, Class<? extends Throwable> expectedCause) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (expectedCause.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }

  private static String unavailableUri() throws IOException {
    try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
      return "http://127.0.0.1:" + socket.getLocalPort();
    }
  }
}
