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
package org.apache.gravitino.policy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;

/**
 * Validates {@code rewrite_data_files} option names and values at policy-save time.
 *
 * <p>Option names mirror Iceberg {@code RewriteDataFiles} / {@code SizeBasedFileRewriter} / {@code
 * SizeBasedDataRewriter} constants so unknown keys fail before a Spark job runs.
 */
final class IcebergRewriteDataFilesOptionValidator {

  private enum ValueKind {
    POSITIVE_LONG,
    POSITIVE_INT,
    NON_NEGATIVE_INT,
    BOOLEAN,
    REWRITE_JOB_ORDER,
    RATIO
  }

  private static final Set<String> REWRITE_JOB_ORDERS =
      ImmutableSet.of("bytes-asc", "bytes-desc", "files-asc", "files-desc", "none");

  private static final Map<String, ValueKind> SUPPORTED_OPTIONS =
      ImmutableMap.<String, ValueKind>builder()
          .put("target-file-size-bytes", ValueKind.POSITIVE_LONG)
          .put("min-file-size-bytes", ValueKind.POSITIVE_LONG)
          .put("max-file-size-bytes", ValueKind.POSITIVE_LONG)
          .put("max-file-group-size-bytes", ValueKind.POSITIVE_LONG)
          .put("min-input-files", ValueKind.POSITIVE_INT)
          .put("max-concurrent-file-group-rewrites", ValueKind.POSITIVE_INT)
          .put("partial-progress.max-commits", ValueKind.POSITIVE_INT)
          .put("partial-progress.max-failed-commits", ValueKind.NON_NEGATIVE_INT)
          .put("output-spec-id", ValueKind.NON_NEGATIVE_INT)
          .put("delete-file-threshold", ValueKind.NON_NEGATIVE_INT)
          .put("rewrite-all", ValueKind.BOOLEAN)
          .put("partial-progress.enabled", ValueKind.BOOLEAN)
          .put("use-starting-sequence-number", ValueKind.BOOLEAN)
          .put("remove-dangling-deletes", ValueKind.BOOLEAN)
          .put("rewrite-job-order", ValueKind.REWRITE_JOB_ORDER)
          .put("delete-ratio-threshold", ValueKind.RATIO)
          .build();

  private IcebergRewriteDataFilesOptionValidator() {}

  static void validate(Map<String, String> rewriteOptions) {
    rewriteOptions.forEach(IcebergRewriteDataFilesOptionValidator::validateEntry);
  }

  private static void validateEntry(String key, String value) {
    Preconditions.checkArgument(StringUtils.isNotBlank(key), "rewrite option key is blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(value), "rewrite option '%s' must have non-empty value", key);
    ValueKind kind = SUPPORTED_OPTIONS.get(key);
    Preconditions.checkArgument(
        kind != null,
        "rewrite option '%s' is not supported by rewrite_data_files; supported options are %s",
        key,
        SUPPORTED_OPTIONS.keySet());
    switch (kind) {
      case POSITIVE_LONG:
        parseLong(
            key, value, v -> Preconditions.checkArgument(v > 0, message(key, "a positive long")));
        break;
      case POSITIVE_INT:
        parseInt(
            key, value, v -> Preconditions.checkArgument(v > 0, message(key, "a positive int")));
        break;
      case NON_NEGATIVE_INT:
        parseInt(
            key,
            value,
            v -> Preconditions.checkArgument(v >= 0, message(key, "a non-negative int")));
        break;
      case BOOLEAN:
        Preconditions.checkArgument(
            "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value),
            message(key, "true or false"));
        break;
      case REWRITE_JOB_ORDER:
        Preconditions.checkArgument(
            REWRITE_JOB_ORDERS.contains(value.toLowerCase(Locale.ROOT)),
            "rewrite option '%s' must be one of %s, but got '%s'",
            key,
            REWRITE_JOB_ORDERS,
            value);
        break;
      case RATIO:
        try {
          double ratio = Double.parseDouble(value.trim());
          Preconditions.checkArgument(
              ratio >= 0.0D && ratio <= 1.0D, message(key, "a number between 0 and 1"));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(message(key, "a number between 0 and 1"), e);
        }
        break;
      default:
        throw new IllegalStateException("Unhandled rewrite option kind: " + kind);
    }
  }

  private static void parseLong(String key, String value, Consumer<Long> check) {
    try {
      check.accept(Long.parseLong(value.trim()));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(message(key, "a long"), e);
    }
  }

  private static void parseInt(String key, String value, Consumer<Integer> check) {
    try {
      check.accept(Integer.parseInt(value.trim()));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(message(key, "an int"), e);
    }
  }

  private static String message(String key, String expected) {
    return String.format("rewrite option '%s' must be %s", key, expected);
  }
}
