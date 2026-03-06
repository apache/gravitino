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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;

/** Built-in policy content for Iceberg compaction strategy. */
public class IcebergCompactionContent implements PolicyContent {
  /** Property key for strategy type. */
  public static final String STRATEGY_TYPE_KEY = "strategy.type";
  /** Strategy type value for compaction. */
  public static final String STRATEGY_TYPE_VALUE = "compaction";
  /** Property key for job template name. */
  public static final String JOB_TEMPLATE_NAME_KEY = "job.template-name";
  /** Built-in job template name for Iceberg rewrite data files. */
  public static final String JOB_TEMPLATE_NAME_VALUE = "builtin-iceberg-rewrite-data-files";
  /** Prefix for rewrite options propagated to job options. */
  public static final String JOB_OPTIONS_PREFIX = "job.options.";
  /** Rule key for trigger expression. */
  public static final String TRIGGER_EXPR_KEY = "trigger-expr";
  /** Rule key for score expression. */
  public static final String SCORE_EXPR_KEY = "score-expr";
  /** Rule key for minimum data file MSE threshold. */
  public static final String MIN_DATA_FILE_MSE_KEY = "minDataFileMse";
  /** Rule key for minimum delete file count threshold. */
  public static final String MIN_DELETE_FILE_NUMBER_KEY = "minDeleteFileNumber";
  /** Rule key for data file MSE score weight. */
  public static final String DATA_FILE_MSE_WEIGHT_KEY = "dataFileMseWeight";
  /** Rule key for delete file number score weight. */
  public static final String DELETE_FILE_NUMBER_WEIGHT_KEY = "deleteFileNumberWeight";
  /** Rule key for max partition number selected for compaction. */
  public static final String MAX_PARTITION_NUM_KEY = "max-partition-num";
  /** Metric name for data file MSE. */
  public static final String DATA_FILE_MSE_METRIC = "custom-data-file-mse";
  /** Metric name for delete file number. */
  public static final String DELETE_FILE_NUMBER_METRIC = "custom-delete-file-number";
  /** Default minimum threshold for data file MSE metric. */
  public static final long DEFAULT_MIN_DATA_FILE_MSE = 405323966463344L;
  /** Default minimum threshold for delete file number metric. */
  public static final long DEFAULT_MIN_DELETE_FILE_NUMBER = 1L;
  /** Default score weight for data file MSE. */
  public static final long DEFAULT_DATA_FILE_MSE_WEIGHT = 1L;
  /** Default score weight for delete file number. */
  public static final long DEFAULT_DELETE_FILE_NUMBER_WEIGHT = 100L;
  /** Default max partition number for compaction. */
  public static final long DEFAULT_MAX_PARTITION_NUM = 50L;
  /** Default rewrite options for Iceberg rewrite data files. */
  public static final Map<String, String> DEFAULT_REWRITE_OPTIONS = ImmutableMap.of();

  private static final Pattern OPTION_KEY_PATTERN = Pattern.compile("[A-Za-z0-9._-]+");
  private static final Set<MetadataObject.Type> SUPPORTED_OBJECT_TYPES =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);
  private static final String TRIGGER_EXPR =
      DATA_FILE_MSE_METRIC
          + " >= "
          + MIN_DATA_FILE_MSE_KEY
          + " || "
          + DELETE_FILE_NUMBER_METRIC
          + " >= "
          + MIN_DELETE_FILE_NUMBER_KEY;
  private static final String SCORE_EXPR =
      DATA_FILE_MSE_METRIC
          + " * "
          + DATA_FILE_MSE_WEIGHT_KEY
          + " + "
          + DELETE_FILE_NUMBER_METRIC
          + " * "
          + DELETE_FILE_NUMBER_WEIGHT_KEY;

  private final Long minDataFileMse;
  private final Long minDeleteFileNumber;
  private final Long dataFileMseWeight;
  private final Long deleteFileNumberWeight;
  private final Long maxPartitionNum;
  private final Map<String, String> rewriteOptions;

  /** Default constructor for Jackson deserialization only. */
  private IcebergCompactionContent() {
    this(null, null, null, null, null, null);
  }

  IcebergCompactionContent(
      Long minDataFileMse,
      Long minDeleteFileNumber,
      Long dataFileMseWeight,
      Long deleteFileNumberWeight,
      Long maxPartitionNum,
      Map<String, String> rewriteOptions) {
    // Nullable inputs are treated as "use default" to simplify policy creation.
    this.minDataFileMse = minDataFileMse == null ? DEFAULT_MIN_DATA_FILE_MSE : minDataFileMse;
    this.minDeleteFileNumber =
        minDeleteFileNumber == null ? DEFAULT_MIN_DELETE_FILE_NUMBER : minDeleteFileNumber;
    this.dataFileMseWeight =
        dataFileMseWeight == null ? DEFAULT_DATA_FILE_MSE_WEIGHT : dataFileMseWeight;
    this.deleteFileNumberWeight =
        deleteFileNumberWeight == null ? DEFAULT_DELETE_FILE_NUMBER_WEIGHT : deleteFileNumberWeight;
    this.maxPartitionNum = maxPartitionNum == null ? DEFAULT_MAX_PARTITION_NUM : maxPartitionNum;
    this.rewriteOptions =
        rewriteOptions == null
            ? DEFAULT_REWRITE_OPTIONS
            : Collections.unmodifiableMap(new LinkedHashMap<>(rewriteOptions));
  }

  /**
   * Returns the minimum threshold for {@value DATA_FILE_MSE_METRIC}.
   *
   * @return minimum data file MSE threshold
   */
  public Long minDataFileMse() {
    return minDataFileMse;
  }

  /**
   * Returns the minimum threshold for {@value DELETE_FILE_NUMBER_METRIC}.
   *
   * @return minimum delete file number threshold
   */
  public Long minDeleteFileNumber() {
    return minDeleteFileNumber;
  }

  /**
   * Returns the weight used by {@value DATA_FILE_MSE_METRIC} in score expression.
   *
   * @return data file MSE score weight
   */
  public Long dataFileMseWeight() {
    return dataFileMseWeight;
  }

  /**
   * Returns the weight used by {@value DELETE_FILE_NUMBER_METRIC} in score expression.
   *
   * @return delete file number score weight
   */
  public Long deleteFileNumberWeight() {
    return deleteFileNumberWeight;
  }

  /**
   * Returns max partition number selected for compaction.
   *
   * @return max partition number
   */
  public Long maxPartitionNum() {
    return maxPartitionNum;
  }

  /**
   * Returns rewrite options that are expanded to {@code job.options.*} rule entries.
   *
   * @return rewrite options
   */
  public Map<String, String> rewriteOptions() {
    return rewriteOptions;
  }

  @Override
  public Set<MetadataObject.Type> supportedObjectTypes() {
    return SUPPORTED_OBJECT_TYPES;
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of(
        STRATEGY_TYPE_KEY, STRATEGY_TYPE_VALUE, JOB_TEMPLATE_NAME_KEY, JOB_TEMPLATE_NAME_VALUE);
  }

  @Override
  public Map<String, Object> rules() {
    Map<String, Object> rules = new LinkedHashMap<>();
    rules.put(MIN_DATA_FILE_MSE_KEY, minDataFileMse);
    rules.put(MIN_DELETE_FILE_NUMBER_KEY, minDeleteFileNumber);
    rules.put(DATA_FILE_MSE_WEIGHT_KEY, dataFileMseWeight);
    rules.put(DELETE_FILE_NUMBER_WEIGHT_KEY, deleteFileNumberWeight);
    rules.put(MAX_PARTITION_NUM_KEY, maxPartitionNum);
    rules.put(TRIGGER_EXPR_KEY, TRIGGER_EXPR);
    rules.put(SCORE_EXPR_KEY, SCORE_EXPR);
    rewriteOptions.forEach((key, value) -> rules.put(JOB_OPTIONS_PREFIX + key, value));
    return Collections.unmodifiableMap(rules);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    PolicyContent.super.validate();
    // All fields are defaulted in the constructor, so only range checks are needed here.
    Preconditions.checkArgument(minDataFileMse >= 0, "minDataFileMse must be >= 0");
    Preconditions.checkArgument(minDeleteFileNumber >= 0, "minDeleteFileNumber must be >= 0");
    Preconditions.checkArgument(dataFileMseWeight >= 0, "dataFileMseWeight must be >= 0");
    Preconditions.checkArgument(deleteFileNumberWeight >= 0, "deleteFileNumberWeight must be >= 0");
    Preconditions.checkArgument(maxPartitionNum > 0, "maxPartitionNum must be > 0");

    rewriteOptions.forEach(
        (key, value) -> {
          Preconditions.checkArgument(StringUtils.isNotBlank(key), "rewrite option key is blank");
          Preconditions.checkArgument(
              OPTION_KEY_PATTERN.matcher(key).matches(),
              "rewrite option key '%s' contains illegal characters",
              key);
          Preconditions.checkArgument(
              !key.startsWith(JOB_OPTIONS_PREFIX),
              "rewrite option key '%s' must not start with '%s'",
              key,
              JOB_OPTIONS_PREFIX);
          Preconditions.checkArgument(
              StringUtils.isNotBlank(value), "rewrite option '%s' must have non-empty value", key);
        });
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IcebergCompactionContent)) {
      return false;
    }
    IcebergCompactionContent that = (IcebergCompactionContent) o;
    return Objects.equals(minDataFileMse, that.minDataFileMse)
        && Objects.equals(minDeleteFileNumber, that.minDeleteFileNumber)
        && Objects.equals(dataFileMseWeight, that.dataFileMseWeight)
        && Objects.equals(deleteFileNumberWeight, that.deleteFileNumberWeight)
        && Objects.equals(maxPartitionNum, that.maxPartitionNum)
        && Objects.equals(rewriteOptions, that.rewriteOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        minDataFileMse,
        minDeleteFileNumber,
        dataFileMseWeight,
        deleteFileNumberWeight,
        maxPartitionNum,
        rewriteOptions);
  }

  @Override
  public String toString() {
    return "IcebergCompactionContent{"
        + "minDataFileMse="
        + minDataFileMse
        + ", minDeleteFileNumber="
        + minDeleteFileNumber
        + ", dataFileMseWeight="
        + dataFileMseWeight
        + ", deleteFileNumberWeight="
        + deleteFileNumberWeight
        + ", maxPartitionNum="
        + maxPartitionNum
        + ", rewriteOptions="
        + rewriteOptions
        + '}';
  }
}
