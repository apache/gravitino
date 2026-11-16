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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.dto.job.SparkJobTemplateDTO;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Display-masks credentials before a runtime template is returned by an API, placed on an event, or
 * written to a local-executor INFO log. Sensitive-named keys are replaced with {@link
 * HiddenPropertyMaskUtils#MASKED_VALUE}. This is not encryption: the stored runtime template and
 * the command list / map passed to job execution stay plaintext.
 *
 * <p>What {@link #maskSensitiveArguments} masks on a command or argument list:
 *
 * <ul>
 *   <li>Spark template arguments {@code --updater-options} and {@code --spark-conf}. One list
 *       element is one argument, so the JSON is the whole next element (or the text after {@code =}
 *       in {@code --flag=json}). It is not split on whitespace. The same walk applies to a full
 *       process command list (for example {@code spark-submit ...}) used only for logging.
 *   <li>{@code --conf key=value} and {@code --conf=key=value} when {@code key} matches {@link
 *       SecretPropertyUtils#isSensitivePropertyKey(String)}. Local Spark emits template {@code
 *       configs} as {@code --conf}. Non-sensitive {@code --conf} entries stay plaintext.
 * </ul>
 *
 * <p>{@link #maskSensitiveJobConf} masks {@code jobConf} entries {@code updater_options} and {@code
 * spark_conf} on run-job events (placeholder names, not the hyphenated CLI flags).
 *
 * <p>Other arguments ({@code --password}, {@code -c}, {@code --options}) and map fields ({@code
 * environments}, {@code customFields}, Spark {@code configs} as a map) are copied unchanged. A
 * blank value, which is what {@code {{spark_conf:-}}} becomes when unset, is left unchanged.
 */
public final class JobRuntimeTemplateMaskUtils {

  private static final Logger LOG = LoggerFactory.getLogger(JobRuntimeTemplateMaskUtils.class);

  /**
   * Argument flags whose following value is a JSON map that may contain credentials.
   *
   * <ul>
   *   <li>{@code --updater-options} — Gravitino / updater credentials
   *   <li>{@code --spark-conf} — Spark configs, including catalog passwords
   * </ul>
   */
  static final Set<String> JSON_ARGUMENT_FLAGS =
      ImmutableSet.of("--updater-options", "--spark-conf");

  /** Spark-submit flag that carries a single {@code key=value} config from template configs. */
  static final String CONF_ARGUMENT_FLAG = "--conf";

  /**
   * {@code jobConf} keys for the Spark placeholders {@code {{updater_options}}} and {@code
   * {{spark_conf}}}. The CLI flags in {@link #JSON_ARGUMENT_FLAGS} use hyphens and are a different
   * spelling of the same two JSON bags.
   */
  static final Set<String> JSON_JOB_CONF_KEYS = ImmutableSet.of("updater_options", "spark_conf");

  private JobRuntimeTemplateMaskUtils() {}

  /**
   * Returns a copy of {@code jobConf} for run-job events. Only the Spark placeholder values {@code
   * updater_options} and {@code spark_conf} are masked. A top-level key such as {@code password} is
   * copied unchanged, because this method does not scan every entry. Does not mutate the input;
   * callers must pass the original map to job execution.
   *
   * @param jobConf runtime configuration from the runJob caller; may be null
   * @return a masked copy, or an empty map when {@code jobConf} is null
   */
  public static Map<String, String> maskSensitiveJobConf(@Nullable Map<String, String> jobConf) {
    if (jobConf == null) {
      // Align with REST (null jobConf → empty map) so RunJob*Event ImmutableMap.copyOf never NPE.
      return new LinkedHashMap<>();
    }
    Map<String, String> masked = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : jobConf.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key != null && JSON_JOB_CONF_KEYS.contains(key)) {
        masked.put(key, HiddenPropertyMaskUtils.maskSensitiveJsonValue(value));
      } else {
        masked.put(key, value);
      }
    }
    return masked;
  }

  /**
   * Returns a copy of a runtime template with {@link #maskSensitiveArguments} applied to {@code
   * arguments}, or {@code null} when the input is null. Spark {@code configs}, {@code
   * environments}, and {@code customFields} maps are not masked. A Shell template is rebuilt with
   * the same argument walk so a hand-written template that uses these flags does not leak; built-in
   * credential JSON lives on the Spark template.
   *
   * <p>The DTO is rebuilt field by field (there is no DTO copy helper). When adding a field to
   * {@link SparkJobTemplateDTO} or {@link ShellJobTemplateDTO}, update this method and the
   * field-carry-over tests in {@code TestJobRuntimeTemplateMaskUtils}.
   *
   * @param jobTemplateDTO the resolved runtime job template DTO
   * @return a masked copy, or null
   */
  @Nullable
  public static JobTemplateDTO maskSensitiveFields(@Nullable JobTemplateDTO jobTemplateDTO) {
    if (jobTemplateDTO == null) {
      return null;
    }
    List<String> maskedArguments = maskSensitiveArguments(jobTemplateDTO.arguments());
    if (jobTemplateDTO instanceof ShellJobTemplateDTO) {
      ShellJobTemplateDTO shell = (ShellJobTemplateDTO) jobTemplateDTO;
      return ShellJobTemplateDTO.builder()
          .withName(shell.name())
          .withComment(shell.comment())
          .withJobType(shell.jobType())
          .withExecutable(shell.executable())
          .withArguments(maskedArguments)
          .withEnvironments(shell.environments())
          .withCustomFields(shell.customFields())
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
          .withArguments(maskedArguments)
          .withEnvironments(spark.environments())
          .withCustomFields(spark.customFields())
          .withClassName(spark.className())
          .withJars(spark.jars())
          .withFiles(spark.files())
          .withArchives(spark.archives())
          .withConfigs(spark.configs())
          .withAudit(spark.audit())
          .build();
    }
    throw new IllegalArgumentException(
        "Unsupported job template type: " + jobTemplateDTO.jobType());
  }

  /**
   * Returns a copy of {@code arguments} with credential-bearing values masked: the JSON value of
   * {@link #JSON_ARGUMENT_FLAGS}, and {@link #CONF_ARGUMENT_FLAG} entries whose key matches {@link
   * SecretPropertyUtils#isSensitivePropertyKey(String)}. The JSON value is one list element,
   * including any spaces inside the JSON. Other arguments are copied unchanged. Does not mutate the
   * input; callers must pass the original list to process execution and use this copy only for
   * display (API, events, or logs).
   *
   * <p>Masking is per argument: if one JSON value fails, that slot becomes {@link
   * HiddenPropertyMaskUtils#MASKED_VALUE} and later arguments are still walked and masked.
   *
   * @param arguments process command tokens or template arguments; may be null
   * @return a masked copy, or null when {@code arguments} is null
   */
  @Nullable
  public static List<String> maskSensitiveArguments(@Nullable List<String> arguments) {
    if (arguments == null) {
      return null;
    }
    if (arguments.isEmpty()) {
      return arguments;
    }
    List<String> result = new ArrayList<>(arguments.size());
    boolean maskNextJson = false;
    boolean maskNextConf = false;
    for (String arg : arguments) {
      if (maskNextJson) {
        maskNextJson = false;
        if (arg != null && !arg.startsWith("--")) {
          result.add(safeMaskSensitiveJsonValue(arg));
          continue;
        }
        // Missing value for the previous flag; this element may itself be a flag.
      }
      if (maskNextConf) {
        maskNextConf = false;
        if (arg != null && !arg.startsWith("--")) {
          result.add(maskConfKeyValue(arg));
          continue;
        }
        // Missing value for --conf; this element may itself be a flag.
      }
      String equalsForm = maskEqualsForm(arg);
      if (equalsForm != null) {
        result.add(equalsForm);
        continue;
      }
      String confEqualsForm = maskConfEqualsForm(arg);
      if (confEqualsForm != null) {
        result.add(confEqualsForm);
        continue;
      }
      result.add(arg);
      if (arg != null && JSON_ARGUMENT_FLAGS.contains(arg)) {
        maskNextJson = true;
      } else if (CONF_ARGUMENT_FLAG.equals(arg)) {
        maskNextConf = true;
      }
    }
    return result;
  }

  /**
   * @return the argument with its JSON payload masked, or {@code null} when {@code arg} is not
   *     {@code --flag=json} for {@link #JSON_ARGUMENT_FLAGS}
   */
  @Nullable
  private static String maskEqualsForm(@Nullable String arg) {
    if (arg == null) {
      return null;
    }
    for (String flag : JSON_ARGUMENT_FLAGS) {
      String prefix = flag + "=";
      if (arg.startsWith(prefix)) {
        return prefix + safeMaskSensitiveJsonValue(arg.substring(prefix.length()));
      }
    }
    return null;
  }

  /**
   * @return {@code --conf=...} with a sensitive value masked, or {@code null} when {@code arg} is
   *     not the equals form of {@link #CONF_ARGUMENT_FLAG}
   */
  @Nullable
  private static String maskConfEqualsForm(@Nullable String arg) {
    if (arg == null) {
      return null;
    }
    String prefix = CONF_ARGUMENT_FLAG + "=";
    if (!arg.startsWith(prefix)) {
      return null;
    }
    return prefix + maskConfKeyValue(arg.substring(prefix.length()));
  }

  /**
   * Masks {@code key=value} when {@code key} looks sensitive. Tokens without {@code =}, or with a
   * non-sensitive key, are returned unchanged.
   */
  private static String maskConfKeyValue(String keyValue) {
    int eq = keyValue.indexOf('=');
    if (eq <= 0) {
      return keyValue;
    }
    String key = keyValue.substring(0, eq);
    if (!SecretPropertyUtils.isSensitivePropertyKey(key)) {
      return keyValue;
    }
    return key + "=" + HiddenPropertyMaskUtils.MASKED_VALUE;
  }

  /**
   * Masks one JSON argument value. Unexpected failures become {@link
   * HiddenPropertyMaskUtils#MASKED_VALUE} so the argument walk can continue. Catches {@link
   * Exception} (same scope as {@link HiddenPropertyMaskUtils#maskSensitiveJsonValue}) rather than
   * only unchecked exceptions.
   */
  private static String safeMaskSensitiveJsonValue(String arg) {
    try {
      return HiddenPropertyMaskUtils.maskSensitiveJsonValue(arg);
    } catch (Exception ignored) {
      // Fixed message only: exception text may embed the argument (and credentials).
      // Distinct from HiddenPropertyMaskUtils' malformed-JSON WARN (that path does not rethrow).
      LOG.warn(
          "Unexpected failure while masking a sensitive JSON argument; replacing with placeholder");
      return HiddenPropertyMaskUtils.MASKED_VALUE;
    }
  }
}
