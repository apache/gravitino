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
package org.apache.gravitino.job;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.dto.job.SparkJobTemplateDTO;
import org.apache.gravitino.job.JobTemplate.JobType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobRuntimeTemplateMaskUtils {

  @Test
  void testMaskSensitiveFieldsOnSparkTemplate() {
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withComment("c")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of(
                    "--password=cli-secret",
                    "plain-arg",
                    "--updater-options",
                    "{\"gravitino_uri\":\"http://localhost\",\"password\":\"arg-pw\"}",
                    "--spark-conf",
                    "{\"jdbc.password\":\"arg-jdbc\",\"spark.executor.memory\":\"1g\"}",
                    "--options",
                    "{\"min-input-files\":\"2\",\"api_token\":\"arg-tok\"}",
                    "--catalog",
                    "ice"))
            .withEnvironments(ImmutableMap.of("AUTH_PASSWORD", "env-secret", "CATALOG", "ice"))
            .withCustomFields(
                ImmutableMap.of(
                    "updater_options",
                    "{\"gravitino_uri\":\"http://localhost\",\"password\":\"nested-pw\"}",
                    "update_mode",
                    "stats"))
            .withClassName("org.example.App")
            .withConfigs(
                ImmutableMap.of(
                    "spark.master",
                    "local[*]",
                    "spark_conf",
                    "{\"jdbc.password\":\"jdbc-pw\",\"spark.executor.memory\":\"1g\"}"))
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    Assertions.assertInstanceOf(SparkJobTemplateDTO.class, masked);
    SparkJobTemplateDTO spark = (SparkJobTemplateDTO) masked;

    List<String> args = spark.arguments();
    // Sensitive-named long options are masked; nest-flag JSON values are walked.
    Assertions.assertEquals("--password=" + HiddenPropertyMaskUtils.MASKED_VALUE, args.get(0));
    Assertions.assertEquals("plain-arg", args.get(1));
    Assertions.assertEquals("--updater-options", args.get(2));
    Assertions.assertFalse(args.get(3).contains("arg-pw"));
    Assertions.assertTrue(args.get(3).contains(HiddenPropertyMaskUtils.MASKED_VALUE));
    Assertions.assertTrue(args.get(3).contains("http://localhost"));
    Assertions.assertEquals("--spark-conf", args.get(4));
    Assertions.assertFalse(args.get(5).contains("arg-jdbc"));
    Assertions.assertTrue(args.get(5).contains("1g"));
    Assertions.assertEquals("--options", args.get(6));
    Assertions.assertFalse(args.get(7).contains("arg-tok"));
    Assertions.assertTrue(args.get(7).contains("2"));
    Assertions.assertEquals("--catalog", args.get(8));
    Assertions.assertEquals("ice", args.get(9));

    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE, spark.environments().get("AUTH_PASSWORD"));
    Assertions.assertEquals("ice", spark.environments().get("CATALOG"));
    Assertions.assertFalse(spark.customFields().get("updater_options").contains("nested-pw"));
    Assertions.assertTrue(
        spark.customFields().get("updater_options").contains(HiddenPropertyMaskUtils.MASKED_VALUE));
    Assertions.assertEquals("stats", spark.customFields().get("update_mode"));
    Assertions.assertFalse(spark.configs().get("spark_conf").contains("jdbc-pw"));
    Assertions.assertTrue(spark.configs().get("spark_conf").contains("1g"));
  }

  @Test
  void testMaskSensitiveArgumentsSkipsMissingValue() {
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(ImmutableList.of("--spark-conf", "--catalog", "ice"))
            .withClassName("org.example.App")
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    SparkJobTemplateDTO spark = (SparkJobTemplateDTO) masked;
    Assertions.assertEquals(
        ImmutableList.of("--spark-conf", "--catalog", "ice"), spark.arguments());
  }

  @Test
  void testMaskSensitiveArgumentsLeavesEmptyTokenUnchanged() {
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of("--spark-conf", "", "--options", "   ", "--catalog", "ice"))
            .withClassName("org.example.App")
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    SparkJobTemplateDTO spark = (SparkJobTemplateDTO) masked;
    Assertions.assertEquals(
        ImmutableList.of("--spark-conf", "", "--options", "   ", "--catalog", "ice"),
        spark.arguments());
  }

  @Test
  void testMaskSensitiveArgumentsMasksEqualsForm() {
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of(
                    "--spark-conf={\"jdbc.password\":\"arg-jdbc\",\"spark.executor.memory\":\"1g\"}",
                    "--options=",
                    "--catalog",
                    "ice"))
            .withClassName("org.example.App")
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    SparkJobTemplateDTO spark = (SparkJobTemplateDTO) masked;
    List<String> args = spark.arguments();
    Assertions.assertTrue(args.get(0).startsWith("--spark-conf="));
    Assertions.assertFalse(args.get(0).contains("arg-jdbc"));
    Assertions.assertTrue(args.get(0).contains("1g"));
    Assertions.assertTrue(args.get(0).contains(HiddenPropertyMaskUtils.MASKED_VALUE));
    Assertions.assertEquals("--options=", args.get(1));
    Assertions.assertEquals("--catalog", args.get(2));
    Assertions.assertEquals("ice", args.get(3));
  }

  @Test
  void testMaskSensitiveJobConf() {
    Map<String, String> jobConf =
        ImmutableMap.of(
            "password",
            "plain-pw",
            "spark_conf",
            "{\"jdbc.password\":\"jdbc-pw\",\"spark.executor.memory\":\"1g\"}",
            "plain",
            "ok");

    Map<String, String> masked = JobRuntimeTemplateMaskUtils.maskSensitiveJobConf(jobConf);
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, masked.get("password"));
    Assertions.assertEquals("ok", masked.get("plain"));
    Assertions.assertFalse(masked.get("spark_conf").contains("jdbc-pw"));
    Assertions.assertTrue(masked.get("spark_conf").contains("1g"));
    Assertions.assertNull(JobRuntimeTemplateMaskUtils.maskSensitiveJobConf(null));
  }

  @Test
  void testMaskSensitiveArgumentsMasksSensitiveNamedFlags() {
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of(
                    "--password",
                    "plain-pw",
                    "--access-key=ak-secret",
                    "--catalog",
                    "ice",
                    "--conf",
                    "jdbc.password=jdbc-pw",
                    "--conf=spark.hadoop.fs.s3a.access.key=AKIAX",
                    "-c=jdbc.password=short-c",
                    "--conf",
                    "spark.executor.memory=1g",
                    "-Dapi.token=tok-secret",
                    "-Dspark.executor.memory=1g"))
            .withClassName("org.example.App")
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    SparkJobTemplateDTO spark = (SparkJobTemplateDTO) masked;
    List<String> args = spark.arguments();
    Assertions.assertEquals("--password", args.get(0));
    Assertions.assertEquals(HiddenPropertyMaskUtils.MASKED_VALUE, args.get(1));
    Assertions.assertEquals("--access-key=" + HiddenPropertyMaskUtils.MASKED_VALUE, args.get(2));
    Assertions.assertEquals("--catalog", args.get(3));
    Assertions.assertEquals("ice", args.get(4));
    Assertions.assertEquals("--conf", args.get(5));
    Assertions.assertEquals("jdbc.password=" + HiddenPropertyMaskUtils.MASKED_VALUE, args.get(6));
    Assertions.assertEquals(
        "--conf=spark.hadoop.fs.s3a.access.key=" + HiddenPropertyMaskUtils.MASKED_VALUE,
        args.get(7));
    Assertions.assertEquals(
        "-c=jdbc.password=" + HiddenPropertyMaskUtils.MASKED_VALUE, args.get(8));
    Assertions.assertEquals("--conf", args.get(9));
    Assertions.assertEquals("spark.executor.memory=1g", args.get(10));
    Assertions.assertEquals("-Dapi.token=" + HiddenPropertyMaskUtils.MASKED_VALUE, args.get(11));
    Assertions.assertEquals("-Dspark.executor.memory=1g", args.get(12));
  }

  @Test
  void testMaskSensitiveArgumentsLeavesWherePredicateUnchanged() {
    String where = "account_id = 5 and dt = '2024-01-01'";
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of(
                    "--catalog",
                    "iceberg_prod",
                    "--table",
                    "db.events",
                    "--where",
                    where,
                    "--options",
                    "{\"min-input-files\":\"2\"}"))
            .withClassName("org.example.App")
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    SparkJobTemplateDTO spark = (SparkJobTemplateDTO) masked;
    List<String> args = spark.arguments();
    Assertions.assertEquals("--where", args.get(4));
    Assertions.assertEquals(where, args.get(5));
  }

  @Test
  void testMaskSensitiveArgumentsMasksJvmOptionsStrings() {
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of(
                    "--driver-java-options",
                    "-Xmx1g -Dpassword=secret -XX:+UseG1GC",
                    "--executor-java-options=-Dapi.token=tok -Xms512m",
                    "--driver-java-options",
                    "--catalog",
                    "ice",
                    "--conf",
                    "--catalog",
                    "ice2"))
            .withClassName("org.example.App")
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    SparkJobTemplateDTO spark = (SparkJobTemplateDTO) masked;
    List<String> args = spark.arguments();
    Assertions.assertEquals("--driver-java-options", args.get(0));
    Assertions.assertEquals(
        "-Xmx1g -Dpassword=" + HiddenPropertyMaskUtils.MASKED_VALUE + " -XX:+UseG1GC", args.get(1));
    Assertions.assertEquals(
        "--executor-java-options=-Dapi.token=" + HiddenPropertyMaskUtils.MASKED_VALUE + " -Xms512m",
        args.get(2));
    // Missing JVM payload then a long option — do not swallow --catalog.
    Assertions.assertEquals("--driver-java-options", args.get(3));
    Assertions.assertEquals("--catalog", args.get(4));
    Assertions.assertEquals("ice", args.get(5));
    // --conf followed by another flag still skips (conf values do not start with '-').
    Assertions.assertEquals("--conf", args.get(6));
    Assertions.assertEquals("--catalog", args.get(7));
    Assertions.assertEquals("ice2", args.get(8));
  }

  @Test
  void testMaskSensitiveArgumentsNestFlagFollowedByEqualsForm() {
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of(
                    "--spark-conf",
                    "--options={\"password\":\"opt-pw\",\"min-input-files\":\"2\"}"))
            .withClassName("org.example.App")
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    SparkJobTemplateDTO spark = (SparkJobTemplateDTO) masked;
    List<String> args = spark.arguments();
    Assertions.assertEquals("--spark-conf", args.get(0));
    Assertions.assertTrue(args.get(1).startsWith("--options="));
    Assertions.assertFalse(args.get(1).contains("opt-pw"));
    Assertions.assertTrue(args.get(1).contains("2"));
    Assertions.assertTrue(args.get(1).contains(HiddenPropertyMaskUtils.MASKED_VALUE));
  }

  @Test
  void testMaskSensitiveFieldsOnShellTemplate() {
    ShellJobTemplateDTO input =
        ShellJobTemplateDTO.builder()
            .withName("shell_job")
            .withJobType(JobType.SHELL)
            .withExecutable("/bin/echo")
            .withArguments(ImmutableList.of("hello"))
            .withEnvironments(ImmutableMap.of("API_TOKEN", "tok"))
            .withCustomFields(ImmutableMap.of("plain", "v"))
            .build();

    JobTemplateDTO masked = JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    Assertions.assertInstanceOf(ShellJobTemplateDTO.class, masked);
    ShellJobTemplateDTO shell = (ShellJobTemplateDTO) masked;

    Assertions.assertEquals(
        HiddenPropertyMaskUtils.MASKED_VALUE, shell.environments().get("API_TOKEN"));
    Assertions.assertEquals("v", shell.customFields().get("plain"));
    Assertions.assertEquals(ImmutableList.of("hello"), shell.arguments());
  }

  @Test
  void testMaskSensitiveFieldsNull() {
    Assertions.assertNull(JobRuntimeTemplateMaskUtils.maskSensitiveFields(null));
  }
}
