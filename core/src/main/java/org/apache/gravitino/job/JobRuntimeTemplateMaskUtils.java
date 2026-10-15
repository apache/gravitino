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
import java.util.ArrayList;
import java.util.List;
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
 * <p>Nested JSON string maps are masked for the Iceberg built-in / optimizer keys listed in {@link
 * #NESTED_JSON_MAP_KEYS} on {@code environments} / {@code customFields} / {@code configs} / runJob
 * {@code jobConf}, and for the matching CLI flags in {@link #NESTED_JSON_ARGUMENT_FLAGS} on {@code
 * arguments} (the value token after the flag, or the {@code --flag=json} form). Other argument
 * tokens are left unchanged; do not put secrets in free-form arguments.
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

  /**
   * Iceberg built-in job CLI flags whose following argument is a JSON map with the same masking
   * rules as {@link #NESTED_JSON_MAP_KEYS}.
   */
  static final Set<String> NESTED_JSON_ARGUMENT_FLAGS =
      ImmutableSet.of("--updater-options", "--spark-conf", "--options");

  private JobRuntimeTemplateMaskUtils() {}

  /**
   * Returns a copy of {@code jobConf} with sensitive-named values masked for event / audit paths.
   * Nested JSON under {@link #NESTED_JSON_MAP_KEYS} is walked the same way as template maps. Does
   * not mutate the input; callers must keep the original for job execution.
   *
   * @param jobConf runtime configuration from the runJob caller; may be null
   * @return a masked mutable copy, or null when {@code jobConf} is null
   */
  @Nullable
  public static Map<String, String> maskSensitiveJobConf(@Nullable Map<String, String> jobConf) {
    if (jobConf == null) {
      return null;
    }
    return HiddenPropertyMaskUtils.maskSensitiveNamedProperties(jobConf, NESTED_JSON_MAP_KEYS);
  }

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
          .withArguments(maskSensitiveArguments(shell.arguments()))
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
          .withArguments(maskSensitiveArguments(spark.arguments()))
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

  /**
   * Masks the value token after known Iceberg JSON-map CLI flags, including the single-token {@code
   * --flag=json} form. Other tokens (including free-form strings that happen to look like JSON) are
   * left unchanged.
   */
  @Nullable
  private static List<String> maskSensitiveArguments(@Nullable List<String> arguments) {
    if (arguments == null) {
      return null;
    }
    if (arguments.isEmpty()) {
      return arguments;
    }
    List<String> result = new ArrayList<>(arguments.size());
    boolean maskNext = false;
    for (String arg : arguments) {
      if (maskNext) {
        maskNext = false;
        if (arg != null && !arg.startsWith("--")) {
          result.add(HiddenPropertyMaskUtils.maskSensitiveJsonValue(arg));
          continue;
        }
        // Missing value for the previous flag; this token may itself be a nest flag.
      }
      String equalsForm = maskEqualsNestFlagArgument(arg);
      if (equalsForm != null) {
        result.add(equalsForm);
        continue;
      }
      result.add(arg);
      if (arg != null && NESTED_JSON_ARGUMENT_FLAGS.contains(arg)) {
        maskNext = true;
      }
    }
    return result;
  }

  /**
   * @return masked {@code --flag=json} token, or {@code null} when {@code arg} is not that form
   */
  @Nullable
  private static String maskEqualsNestFlagArgument(@Nullable String arg) {
    if (arg == null) {
      return null;
    }
    for (String flag : NESTED_JSON_ARGUMENT_FLAGS) {
      String prefix = flag + "=";
      if (arg.startsWith(prefix)) {
        String json = arg.substring(prefix.length());
        return prefix + HiddenPropertyMaskUtils.maskSensitiveJsonValue(json);
      }
    }
    return null;
  }
}
