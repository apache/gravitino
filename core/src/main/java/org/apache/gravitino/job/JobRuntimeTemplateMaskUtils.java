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
import org.apache.gravitino.secret.SecretPropertyUtils;

/**
 * Masks sensitive-named values on resolved runtime job templates before they are exposed on API or
 * event paths. Stored runtime JSON is unchanged.
 *
 * <p>Argument masking is grouped by payload shape:
 *
 * <ul>
 *   <li>Nest JSON — {@link #NESTED_JSON_ARGUMENT_FLAGS} ({@code --updater-options} / {@code
 *       --spark-conf} / {@code --options})
 *   <li>Spark conf property — {@link #CONF_ARGUMENT_FLAGS} ({@code --conf} / {@code -c}): a single
 *       whitespace-free {@code key=value} in either two-token or {@code --conf=key=value} form
 *   <li>JVM options string — {@link #JVM_OPTIONS_ARGUMENT_FLAGS} ({@code --driver-java-options} /
 *       {@code --executor-java-options}): a space-separated JVM string that may start with {@code
 *       -}; sensitive {@code -Dkey=value} tokens inside are masked
 *   <li>Sensitive-named long options — {@code --name} / {@code --name=value} when the name matches
 *       {@link SecretPropertyUtils#isSensitivePropertyKey} (the following token after {@code
 *       --name} is always replaced with {@link HiddenPropertyMaskUtils#MASKED_VALUE}, including
 *       non-secret values such as identifiers after {@code --use-credential-file})
 *   <li>Standalone {@code -Dkey=value} when {@code key} is sensitive
 * </ul>
 *
 * <p>Map fields {@code environments} / {@code customFields} / {@code configs} and runJob {@code
 * jobConf} mask sensitive-named keys, plus nested JSON under {@link #NESTED_JSON_MAP_KEYS}.
 *
 * <p>Not masked (out of scope): resource URI fields ({@code executable}, {@code className}, {@code
 * jars}, {@code files}, {@code archives}, Shell {@code scripts}); {@code --hiveconf}; conf property
 * values that embed {@code -D} secrets under a non-sensitive conf key (for example {@code
 * spark.driver.extraJavaOptions}); free-form tokens such as a {@code --where} SQL predicate; Shell
 * {@code /bin/sh -c} command strings that contain whitespace (so they are not treated as a Spark
 * {@code -c key=value} conf entry).
 *
 * <p>Null map entries are preserved in the masked DTO / {@code jobConf} maps used on the REST and
 * event {@code jobConf} paths. When {@code JobInfo} rebuilds a {@code JobTemplate} via {@code
 * ImmutableMap.copyOf}, null values cannot be carried; a hand-edited runtime JSON with nulls may
 * cause that rebuild to be skipped (logged) rather than expose a partial template.
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

  /**
   * Spark flags whose payload is a single {@code key=value} config entry. Both the two-token form
   * ({@code --conf key=value}) and the equals form ({@code --conf=key=value}) use {@link
   * #maskConfKeyValuePayload}.
   *
   * <p>{@code -c} is also the shell {@code /bin/sh -c} flag. Conf masking only applies when the
   * payload looks like one Spark conf entry (no whitespace); shell command strings are left
   * unchanged.
   */
  static final Set<String> CONF_ARGUMENT_FLAGS = ImmutableSet.of("--conf", "-c");

  /**
   * Spark flags whose payload is a space-separated JVM options string (often starting with {@code
   * -X} / {@code -D}). Both two-token and equals forms use {@link #maskJvmOptionsPayload}.
   */
  static final Set<String> JVM_OPTIONS_ARGUMENT_FLAGS =
      ImmutableSet.of("--driver-java-options", "--executor-java-options");

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
   * Masks nest-flag JSON values, sensitive-named long options, conf {@code key=value} payloads, JVM
   * options strings, and standalone {@code -Dkey=value} tokens. Other tokens are left unchanged.
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
    boolean maskNextAsNestedJson = false;
    boolean maskNextAsSecret = false;
    boolean maskNextAsConfPayload = false;
    boolean maskNextAsJvmOptions = false;
    for (String arg : arguments) {
      if (maskNextAsNestedJson) {
        maskNextAsNestedJson = false;
        if (arg != null && !arg.startsWith("--")) {
          result.add(HiddenPropertyMaskUtils.maskSensitiveJsonValue(arg));
          continue;
        }
        // Missing value for the previous nest flag; this token may itself be a flag.
      }
      if (maskNextAsSecret) {
        maskNextAsSecret = false;
        if (arg != null && !arg.startsWith("--")) {
          result.add(HiddenPropertyMaskUtils.MASKED_VALUE);
          continue;
        }
        // Missing value for the previous sensitive flag; this token may itself be a flag.
      }
      if (maskNextAsConfPayload) {
        maskNextAsConfPayload = false;
        if (arg != null && !arg.startsWith("-")) {
          result.add(maskConfKeyValuePayload(arg));
          continue;
        }
        // Missing conf value or next token is another flag; fall through.
      }
      if (maskNextAsJvmOptions) {
        maskNextAsJvmOptions = false;
        if (arg != null && !isKnownStandaloneFlag(arg)) {
          // JVM strings often start with '-' (e.g. -Xmx / -D); only skip known flags.
          result.add(maskJvmOptionsPayload(arg));
          continue;
        }
        // Missing JVM options value; this token may itself be a flag.
      }

      String nestEquals = maskEqualsNestFlagArgument(arg);
      if (nestEquals != null) {
        result.add(nestEquals);
        continue;
      }
      String confEquals = maskEqualsConfFlagArgument(arg);
      if (confEquals != null) {
        result.add(confEquals);
        continue;
      }
      String jvmEquals = maskEqualsJvmOptionsFlagArgument(arg);
      if (jvmEquals != null) {
        result.add(jvmEquals);
        continue;
      }
      String sensitiveEquals = maskEqualsSensitiveFlagArgument(arg);
      if (sensitiveEquals != null) {
        result.add(sensitiveEquals);
        continue;
      }
      String dashD = maskSensitiveDashDArgument(arg);
      if (dashD != null) {
        result.add(dashD);
        continue;
      }

      result.add(arg);
      if (arg != null && NESTED_JSON_ARGUMENT_FLAGS.contains(arg)) {
        maskNextAsNestedJson = true;
      } else if (arg != null && CONF_ARGUMENT_FLAGS.contains(arg)) {
        maskNextAsConfPayload = true;
      } else if (arg != null && JVM_OPTIONS_ARGUMENT_FLAGS.contains(arg)) {
        maskNextAsJvmOptions = true;
      } else if (isSensitiveLongOptionFlag(arg)) {
        maskNextAsSecret = true;
      }
    }
    return result;
  }

  /**
   * Whether {@code arg} is a known flag that must not be consumed as a JVM-options payload when it
   * immediately follows {@code --driver-java-options} / {@code --executor-java-options}.
   */
  private static boolean isKnownStandaloneFlag(@Nullable String arg) {
    if (arg == null) {
      return false;
    }
    if (NESTED_JSON_ARGUMENT_FLAGS.contains(arg)
        || CONF_ARGUMENT_FLAGS.contains(arg)
        || JVM_OPTIONS_ARGUMENT_FLAGS.contains(arg)) {
      return true;
    }
    // Other long options (including non-sensitive ones such as --catalog).
    return arg.startsWith("--") && arg.length() > 2 && arg.indexOf('=') < 0;
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

  /**
   * @return masked {@code --conf=key=value} (or {@code -c=}) token, or {@code null} when {@code
   *     arg} is not that form
   */
  @Nullable
  private static String maskEqualsConfFlagArgument(@Nullable String arg) {
    if (arg == null) {
      return null;
    }
    for (String flag : CONF_ARGUMENT_FLAGS) {
      String prefix = flag + "=";
      if (arg.startsWith(prefix)) {
        String payload = arg.substring(prefix.length());
        return prefix + maskConfKeyValuePayload(payload);
      }
    }
    return null;
  }

  /**
   * @return masked {@code --driver-java-options=...} / {@code --executor-java-options=...} token,
   *     or {@code null} when {@code arg} is not that form
   */
  @Nullable
  private static String maskEqualsJvmOptionsFlagArgument(@Nullable String arg) {
    if (arg == null) {
      return null;
    }
    for (String flag : JVM_OPTIONS_ARGUMENT_FLAGS) {
      String prefix = flag + "=";
      if (arg.startsWith(prefix)) {
        String payload = arg.substring(prefix.length());
        return prefix + maskJvmOptionsPayload(payload);
      }
    }
    return null;
  }

  /**
   * Masks a conf-style {@code key=value} payload when {@code key} is sensitive. Shared by the
   * two-token ({@code --conf key=value}) and equals ({@code --conf=key=value}) forms.
   *
   * <p>Payloads that contain whitespace are left unchanged. A Spark conf entry is a single token
   * without spaces; a Shell {@code -c} command string almost always contains spaces, so this keeps
   * {@code /bin/sh -c "export ACCOUNT_ID=5 && run.sh"} from being truncated as a conf entry.
   *
   * @param payload the config entry after the conf flag
   * @return masked payload, or {@code payload} unchanged when not a sensitive whitespace-free
   *     {@code key=value}
   */
  private static String maskConfKeyValuePayload(String payload) {
    // Spark --conf / -c values are a single key=value token; shell -c commands contain spaces.
    if (payload.isEmpty() || payload.chars().anyMatch(Character::isWhitespace)) {
      return payload;
    }
    int eq = payload.indexOf('=');
    if (eq <= 0) {
      return payload;
    }
    String key = payload.substring(0, eq);
    if (!SecretPropertyUtils.isSensitivePropertyKey(key)) {
      return payload;
    }
    return key + "=" + HiddenPropertyMaskUtils.MASKED_VALUE;
  }

  /**
   * Masks sensitive {@code -Dkey=value} tokens inside a space-separated JVM options string.
   *
   * @param payload JVM options after {@code --driver-java-options} / {@code
   *     --executor-java-options}
   * @return masked payload, or {@code payload} unchanged when nothing was sensitive
   */
  private static String maskJvmOptionsPayload(String payload) {
    if (payload.isEmpty()) {
      return payload;
    }
    String[] parts = payload.trim().split("\\s+");
    boolean changed = false;
    List<String> out = new ArrayList<>(parts.length);
    for (String part : parts) {
      if (part.isEmpty()) {
        continue;
      }
      String maskedDashD = maskSensitiveDashDArgument(part);
      if (maskedDashD != null) {
        out.add(maskedDashD);
        changed = true;
      } else {
        out.add(part);
      }
    }
    return changed ? String.join(" ", out) : payload;
  }

  /**
   * @return masked {@code --sensitiveName=value}, or {@code null} when not that form
   */
  @Nullable
  private static String maskEqualsSensitiveFlagArgument(@Nullable String arg) {
    if (arg == null || !arg.startsWith("--")) {
      return null;
    }
    int eq = arg.indexOf('=');
    if (eq <= 2) {
      return null;
    }
    String name = arg.substring(2, eq);
    if (!SecretPropertyUtils.isSensitivePropertyKey(name)) {
      return null;
    }
    return arg.substring(0, eq + 1) + HiddenPropertyMaskUtils.MASKED_VALUE;
  }

  /** {@code --password} / {@code --access-key} style flags (no {@code =}). */
  private static boolean isSensitiveLongOptionFlag(@Nullable String arg) {
    if (arg == null || !arg.startsWith("--") || arg.length() <= 2 || arg.indexOf('=') >= 0) {
      return false;
    }
    return SecretPropertyUtils.isSensitivePropertyKey(arg.substring(2));
  }

  /**
   * Masks {@code -DsensitiveKey=value} (common JVM / Spark style).
   *
   * @return masked token, or {@code null} when not that form
   */
  @Nullable
  private static String maskSensitiveDashDArgument(@Nullable String arg) {
    if (arg == null || !arg.startsWith("-D") || arg.length() <= 2) {
      return null;
    }
    String rest = arg.substring(2);
    int eq = rest.indexOf('=');
    if (eq <= 0) {
      return null;
    }
    String key = rest.substring(0, eq);
    if (!SecretPropertyUtils.isSensitivePropertyKey(key)) {
      return null;
    }
    return "-D" + key + "=" + HiddenPropertyMaskUtils.MASKED_VALUE;
  }
}
