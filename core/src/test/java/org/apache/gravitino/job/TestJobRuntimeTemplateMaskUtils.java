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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.dto.job.SparkJobTemplateDTO;
import org.apache.gravitino.job.JobTemplate.JobType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobRuntimeTemplateMaskUtils {

  @Test
  void testMaskSensitiveFieldsMasksUpdaterOptionsAndSparkConf() {
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withComment("c")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of(
                    "--catalog",
                    "ice",
                    "--updater-options",
                    "{\"gravitino_uri\":\"http://localhost\",\"password\":\"arg-pw\","
                        + "\"oauth_token\":\"tok\"}",
                    "--spark-conf",
                    "{\"spark.executor.memory\":\"1g\","
                        + "\"spark.sql.catalog.ice.rest.auth.basic.password\":\"jdbc-pw\"}",
                    "--table",
                    "db.t"))
            .withEnvironments(ImmutableMap.of("AUTH_PASSWORD", "env-secret", "CATALOG", "ice"))
            .withClassName("org.example.App")
            .withConfigs(ImmutableMap.of("spark.hadoop.fs.s3a.secret.key", "s3-secret"))
            .build();

    SparkJobTemplateDTO spark =
        (SparkJobTemplateDTO) JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    List<String> args = spark.arguments();
    Assertions.assertEquals("--updater-options", args.get(2));
    Assertions.assertFalse(args.get(3).contains("arg-pw"));
    Assertions.assertFalse(args.get(3).contains("\"tok\""));
    Assertions.assertTrue(args.get(3).contains("http://localhost"));
    Assertions.assertTrue(args.get(3).contains(HiddenPropertyMaskUtils.MASKED_VALUE));
    Assertions.assertEquals("--spark-conf", args.get(4));
    Assertions.assertFalse(args.get(5).contains("jdbc-pw"));
    Assertions.assertTrue(args.get(5).contains("1g"));
    Assertions.assertEquals("--table", args.get(6));
    Assertions.assertEquals("db.t", args.get(7));
    // Map fields are outside this fix.
    Assertions.assertEquals("env-secret", spark.environments().get("AUTH_PASSWORD"));
    Assertions.assertEquals("s3-secret", spark.configs().get("spark.hadoop.fs.s3a.secret.key"));
  }

  @Test
  void testMaskSensitiveArgumentsLeavesEmptyTokenUnchanged() {
    SparkJobTemplateDTO input =
        sparkWithArguments(ImmutableList.of("--spark-conf", "", "--updater-options", "   "));

    SparkJobTemplateDTO spark =
        (SparkJobTemplateDTO) JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    Assertions.assertEquals(
        ImmutableList.of("--spark-conf", "", "--updater-options", "   "), spark.arguments());
  }

  @Test
  void testMaskSensitiveArgumentsSkipsMissingValue() {
    SparkJobTemplateDTO input =
        sparkWithArguments(ImmutableList.of("--spark-conf", "--catalog", "ice"));

    SparkJobTemplateDTO spark =
        (SparkJobTemplateDTO) JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    Assertions.assertEquals(
        ImmutableList.of("--spark-conf", "--catalog", "ice"), spark.arguments());
  }

  @Test
  void testMaskSensitiveArgumentsMasksEqualsForm() {
    SparkJobTemplateDTO input =
        sparkWithArguments(
            ImmutableList.of(
                "--spark-conf={\"jdbc.password\":\"arg-jdbc\",\"spark.executor.memory\":\"1g\"}",
                "--updater-options="));

    SparkJobTemplateDTO spark =
        (SparkJobTemplateDTO) JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    List<String> args = spark.arguments();
    Assertions.assertTrue(args.get(0).startsWith("--spark-conf="));
    Assertions.assertFalse(args.get(0).contains("arg-jdbc"));
    Assertions.assertTrue(args.get(0).contains("1g"));
    Assertions.assertEquals("--updater-options=", args.get(1));
  }

  @Test
  void testMaskSensitiveArgumentsLeavesOtherFlagsUnchanged() {
    String where = "account_id = 5 and dt = '2024-01-01'";
    String command = "export db_password=pd && run.sh";
    ShellJobTemplateDTO input =
        ShellJobTemplateDTO.builder()
            .withName("shell_job")
            .withJobType(JobType.SHELL)
            .withExecutable("/bin/sh")
            .withArguments(
                ImmutableList.of(
                    "-c",
                    command,
                    "--password",
                    "my secret pass",
                    "--conf",
                    "spark.jdbc.password=my pass",
                    "--where",
                    where))
            .build();

    ShellJobTemplateDTO shell =
        (ShellJobTemplateDTO) JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);
    Assertions.assertEquals(
        ImmutableList.of(
            "-c",
            command,
            "--password",
            "my secret pass",
            "--conf",
            "spark.jdbc.password=my pass",
            "--where",
            where),
        shell.arguments());
  }

  @Test
  void testMaskSensitiveFieldsNull() {
    Assertions.assertNull(JobRuntimeTemplateMaskUtils.maskSensitiveFields(null));
  }

  @Test
  void testMaskSensitiveFieldsCarriesAllSparkDtoFields() {
    AuditDTO audit = AuditDTO.builder().withCreator("alice").build();
    SparkJobTemplateDTO input =
        SparkJobTemplateDTO.builder()
            .withName("spark_job")
            .withComment("keep-comment")
            .withJobType(JobType.SPARK)
            .withExecutable("local:///app.jar")
            .withArguments(
                ImmutableList.of("--updater-options", "{\"password\":\"pw\"}", "--table", "db.t"))
            .withEnvironments(ImmutableMap.of("K", "v"))
            .withCustomFields(ImmutableMap.of("cf", "1"))
            .withClassName("org.example.App")
            .withJars(ImmutableList.of("a.jar"))
            .withFiles(ImmutableList.of("f.txt"))
            .withArchives(ImmutableList.of("a.zip"))
            .withConfigs(ImmutableMap.of("spark.executor.memory", "1g"))
            .withAudit(audit)
            .build();

    SparkJobTemplateDTO spark =
        (SparkJobTemplateDTO) JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);

    Assertions.assertEquals(input.name(), spark.name());
    Assertions.assertEquals(input.comment(), spark.comment());
    Assertions.assertEquals(input.jobType(), spark.jobType());
    Assertions.assertEquals(input.executable(), spark.executable());
    Assertions.assertEquals(input.environments(), spark.environments());
    Assertions.assertEquals(input.customFields(), spark.customFields());
    Assertions.assertEquals(input.className(), spark.className());
    Assertions.assertEquals(input.jars(), spark.jars());
    Assertions.assertEquals(input.files(), spark.files());
    Assertions.assertEquals(input.archives(), spark.archives());
    Assertions.assertEquals(input.configs(), spark.configs());
    Assertions.assertEquals(input.audit(), spark.audit());
    Assertions.assertNotEquals(input.arguments(), spark.arguments());
    Assertions.assertFalse(spark.arguments().get(1).contains("pw"));
  }

  @Test
  void testMaskSensitiveFieldsCarriesAllShellDtoFields() {
    AuditDTO audit = AuditDTO.builder().withCreator("bob").build();
    ShellJobTemplateDTO input =
        ShellJobTemplateDTO.builder()
            .withName("shell_job")
            .withComment("shell-comment")
            .withJobType(JobType.SHELL)
            .withExecutable("/bin/sh")
            .withArguments(ImmutableList.of("-c", "echo ok"))
            .withEnvironments(ImmutableMap.of("HOME", "/tmp"))
            .withCustomFields(ImmutableMap.of("owner", "tms"))
            .withScripts(ImmutableList.of("a.sh"))
            .withAudit(audit)
            .build();

    ShellJobTemplateDTO shell =
        (ShellJobTemplateDTO) JobRuntimeTemplateMaskUtils.maskSensitiveFields(input);

    Assertions.assertEquals(input.name(), shell.name());
    Assertions.assertEquals(input.comment(), shell.comment());
    Assertions.assertEquals(input.jobType(), shell.jobType());
    Assertions.assertEquals(input.executable(), shell.executable());
    Assertions.assertEquals(input.arguments(), shell.arguments());
    Assertions.assertEquals(input.environments(), shell.environments());
    Assertions.assertEquals(input.customFields(), shell.customFields());
    Assertions.assertEquals(input.scripts(), shell.scripts());
    Assertions.assertEquals(input.audit(), shell.audit());
  }

  @Test
  void testMaskSensitiveJobConfMasksUpdaterOptionsAndSparkConf() {
    Map<String, String> jobConf = new LinkedHashMap<>();
    jobConf.put("catalog_name", "ice");
    jobConf.put(
        "updater_options",
        "{\"gravitino_uri\":\"http://localhost\",\"password\":\"arg-pw\",\"oauth_token\":\"tok\"}");
    jobConf.put(
        "spark_conf",
        "{\"spark.executor.memory\":\"1g\","
            + "\"spark.sql.catalog.ice.rest.auth.basic.password\":\"jdbc-pw\"}");
    jobConf.put("AUTH_PASSWORD", "env-secret");

    Map<String, String> masked = JobRuntimeTemplateMaskUtils.maskSensitiveJobConf(jobConf);

    Assertions.assertEquals("ice", masked.get("catalog_name"));
    Assertions.assertEquals("env-secret", masked.get("AUTH_PASSWORD"));
    Assertions.assertFalse(masked.get("updater_options").contains("arg-pw"));
    Assertions.assertFalse(masked.get("updater_options").contains("\"tok\""));
    Assertions.assertTrue(masked.get("updater_options").contains("http://localhost"));
    Assertions.assertFalse(masked.get("spark_conf").contains("jdbc-pw"));
    Assertions.assertTrue(masked.get("spark_conf").contains("1g"));
    Assertions.assertTrue(jobConf.get("updater_options").contains("arg-pw"));
    Assertions.assertNull(JobRuntimeTemplateMaskUtils.maskSensitiveJobConf(null));
  }

  private static SparkJobTemplateDTO sparkWithArguments(List<String> arguments) {
    return SparkJobTemplateDTO.builder()
        .withName("spark_job")
        .withJobType(JobType.SPARK)
        .withExecutable("local:///app.jar")
        .withArguments(arguments)
        .withClassName("org.example.App")
        .build();
  }
}
