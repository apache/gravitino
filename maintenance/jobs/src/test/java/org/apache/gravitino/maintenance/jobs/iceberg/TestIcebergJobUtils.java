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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

public class TestIcebergJobUtils {

  @Test
  public void testStrictArgumentsNormalizeTemplateValues() {
    Map<String, String> parsed =
        IcebergJobUtils.parseArguments(
            new String[] {"--required", " value ", "--optional", " {{optional}} "},
            new HashSet<>(Arrays.asList("required", "optional")),
            Collections.singleton("required"));
    assertEquals("value", parsed.get("required"));
    assertNull(parsed.get("optional"));
  }

  @Test
  public void testStrictArgumentsRejectMalformedInput() {
    Set<String> supported = Collections.singleton("key");
    String[][] invalid = {
      {"--unknown", "value"},
      {"key", "value"},
      {null, "value"},
      {"--key"},
      {"--key", null},
      {"--key", "--key"},
      {"--key", "", "--key", "value"},
      {"--key", "{{key}}"},
      {"--key", " "},
      {}
    };
    for (String[] args : invalid) {
      assertThrows(
          IllegalArgumentException.class,
          () -> IcebergJobUtils.parseArguments(args, supported, supported));
    }
  }

  @Test
  public void testLegacyArgumentsStillAcceptBareFlags() {
    assertEquals("true", IcebergJobUtils.parseArguments(new String[] {"--flag"}).get("flag"));
  }

  @Test
  public void testRequireIcebergSparkRuntimeSucceedsWhenPresent() {
    // Test classpath includes iceberg-spark-runtime.
    assertDoesNotThrow(IcebergJobUtils::requireIcebergSparkRuntime);
  }

  @Test
  public void testRequireClassFailsWithActionableMessage() {
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                IcebergJobUtils.requireClassForTest(
                    "org.apache.gravitino.does.not.ExistIcebergExtension",
                    "Iceberg Spark session extensions"));
    assertTrue(ex.getMessage().contains("Missing Iceberg Spark session extensions"));
    assertTrue(ex.getMessage().contains("iceberg-spark-runtime"));
    assertTrue(ex.getMessage().contains("spark.jars"));
  }

  @Test
  public void testParseCustomSparkConfigsUsesSparkConfFlagName() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergJobUtils.parseCustomSparkConfigs("{not_json}"));
    assertTrue(ex.getMessage().contains("--spark-conf"));
  }

  @Test
  public void testFilterCustomSparkConfigsDropsSubmittedMasterAndDeployMode() {
    Map<String, String> configs =
        ImmutableMap.of(
            "spark.master", "local[*]",
            "spark.submit.deployMode", "client",
            "spark.jars", "/opt/iceberg-spark-runtime.jar",
            "spark.sql.shuffle.partitions", "200");
    SparkConf submitted =
        new SparkConf(false)
            .set("spark.master", "k8s://https://kubernetes.default.svc")
            .set("spark.submit.deployMode", "cluster");

    assertEquals(
        ImmutableMap.of(
            "spark.jars", "/opt/iceberg-spark-runtime.jar", "spark.sql.shuffle.partitions", "200"),
        IcebergJobUtils.filterCustomSparkConfigs(configs, submitted));
  }

  @Test
  public void testFilterCustomSparkConfigsKeepsMasterNotSubmitted() {
    // A job run without spark-submit, e.g. in a test, has no master unless it sets one.
    Map<String, String> configs = ImmutableMap.of("spark.master", "local[1]");
    assertEquals(configs, IcebergJobUtils.filterCustomSparkConfigs(configs, new SparkConf(false)));
  }

  @Test
  public void testFilterCustomSparkConfigsDropsKubernetesAndBlankConfigs() {
    Map<String, String> configs =
        ImmutableMap.of(
            "spark.kubernetes.executor.secretKeyRef.TOKEN", "secret:token",
            "spark.kubernetes.container.image", "other:1.0",
            "spark.sql.shuffle.partitions", "",
            "spark.executor.memory", "2g");
    assertEquals(
        ImmutableMap.of("spark.executor.memory", "2g"),
        IcebergJobUtils.filterCustomSparkConfigs(configs, new SparkConf(false)));
  }
}
