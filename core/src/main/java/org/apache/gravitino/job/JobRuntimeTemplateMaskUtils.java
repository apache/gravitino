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

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.dto.job.SparkJobTemplateDTO;

/**
 * Masks sensitive-named values on resolved runtime job templates before they are exposed on API or
 * event paths. Stored runtime JSON is unchanged.
 *
 * <p>{@code arguments} are not masked: they are positional strings without property keys, so {@link
 * org.apache.gravitino.secret.SecretPropertyUtils} keyword matching cannot apply. Do not put
 * secrets in job arguments; use {@code environments}, {@code configs}, or {@code customFields}
 * instead.
 *
 * <p>Nested JSON string maps are masked only for the Iceberg built-in / optimizer keys listed in
 * {@link #NESTED_JSON_MAP_KEYS} (never by guessing on a leading curly brace).
 */
public final class JobRuntimeTemplateMaskUtils {

  /**
   * JobConf / template keys whose values are known to be JSON maps (Iceberg built-in jobs and the
   * optimizer). Nested sensitive keys inside these values are masked.
   *
   * <ul>
   *   <li>{@code updater_options} — update-stats job credentials / client options
   *   <li>{@code spark_conf} — Spark configs for built-in Iceberg jobs
   *   <li>{@code options} — rewrite-data-files / compaction options (from policy {@code jobOptions}
   *       / {@code rewriteOptions})
   * </ul>
   */
  static final Set<String> NESTED_JSON_MAP_KEYS =
      ImmutableSet.of("updater_options", "spark_conf", "options");

  private JobRuntimeTemplateMaskUtils() {}

  /**
   * Returns a copy of {@code jobTemplateDTO} with sensitive map values masked, or {@code null} when
   * the input is null.
   *
   * @param jobTemplateDTO the resolved runtime job template DTO
   * @return a masked copy, or null
   */
  @Nullable
  public static JobTemplateDTO maskSensitiveFields(@Nullable JobTemplateDTO jobTemplateDTO) {
    if (jobTemplateDTO == null) {
      return null;
    }
    Map<String, String> maskedEnvironments = maskSensitiveMap(jobTemplateDTO.environments());
    Map<String, String> maskedCustomFields = maskSensitiveMap(jobTemplateDTO.customFields());

    if (jobTemplateDTO instanceof ShellJobTemplateDTO) {
      ShellJobTemplateDTO shell = (ShellJobTemplateDTO) jobTemplateDTO;
      return ShellJobTemplateDTO.builder()
          .withName(shell.name())
          .withComment(shell.comment())
          .withJobType(shell.jobType())
          .withExecutable(shell.executable())
          .withArguments(shell.arguments())
          .withEnvironments(maskedEnvironments)
          .withCustomFields(maskedCustomFields)
          .withScripts(shell.scripts())
          .withAudit(shell.audit())
          .build();
    }
    if (jobTemplateDTO instanceof SparkJobTemplateDTO) {
      SparkJobTemplateDTO spark = (SparkJobTemplateDTO) jobTemplateDTO;
      return SparkJobTemplateDTO.builder()
          .withName(spark.name())
          .withComment(spark.comment())
          .withJobType(spark.jobType())
          .withExecutable(spark.executable())
          .withArguments(spark.arguments())
          .withEnvironments(maskedEnvironments)
          .withCustomFields(maskedCustomFields)
          .withClassName(spark.className())
          .withJars(spark.jars())
          .withFiles(spark.files())
          .withArchives(spark.archives())
          .withConfigs(maskSensitiveMap(spark.configs()))
          .withAudit(spark.audit())
          .build();
    }
    throw new IllegalArgumentException(
        "Unsupported job template type: " + jobTemplateDTO.jobType());
  }

  private static Map<String, String> maskSensitiveMap(Map<String, String> properties) {
    return properties == null
        ? null
        : HiddenPropertyMaskUtils.maskSensitiveNamedProperties(properties, NESTED_JSON_MAP_KEYS);
  }
}
