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

/** Utility class for creating instances of {@link PolicyContent}. */
public class PolicyContents {

  /**
   * Creates a custom policy content with the given rules and properties.
   *
   * @param rules The custom rules of the policy.
   * @param supportedObjectTypes The set of metadata object types that the policy can be applied to.
   * @param properties The additional properties of the policy.
   * @return A new instance of {@link PolicyContent} with the specified rules and properties.
   */
  public static PolicyContent custom(
      Map<String, Object> rules,
      Set<MetadataObject.Type> supportedObjectTypes,
      Map<String, String> properties) {
    return new CustomContent(rules, supportedObjectTypes, properties);
  }

  /**
   * Creates an iceberg compaction policy content.
   *
   * @param minDatafileMse minimum threshold for custom-datafile_mse
   * @param minDeleteFileNumber minimum threshold for custom-delete_file_number
   * @param rewriteOptions rewrite options forwarded as job.options.*
   * @return iceberg compaction policy content
   */
  public static PolicyContent icebergCompaction(
      long minDatafileMse, long minDeleteFileNumber, Map<String, String> rewriteOptions) {
    return new IcebergCompactionContent(
        minDatafileMse,
        minDeleteFileNumber,
        IcebergCompactionContent.DEFAULT_DATAFILE_MSE_WEIGHT,
        IcebergCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
        rewriteOptions);
  }

  /**
   * Creates an iceberg compaction policy content with configurable score weights.
   *
   * @param minDatafileMse minimum threshold for custom-datafile_mse
   * @param minDeleteFileNumber minimum threshold for custom-delete_file_number
   * @param datafileMseWeight weight used for custom-datafile_mse score contribution
   * @param deleteFileNumberWeight weight used for custom-delete_file_number score contribution
   * @param rewriteOptions rewrite options forwarded as job.options.*
   * @return iceberg compaction policy content
   */
  public static PolicyContent icebergCompaction(
      long minDatafileMse,
      long minDeleteFileNumber,
      long datafileMseWeight,
      long deleteFileNumberWeight,
      Map<String, String> rewriteOptions) {
    return new IcebergCompactionContent(
        minDatafileMse,
        minDeleteFileNumber,
        datafileMseWeight,
        deleteFileNumberWeight,
        rewriteOptions);
  }

  private PolicyContents() {}

  /**
   * A custom content implementation of {@link PolicyContent} that holds custom rules and
   * properties.
   */
  public static class CustomContent implements PolicyContent {
    private final Map<String, Object> customRules;
    private final Set<MetadataObject.Type> supportedObjectTypes;
    private final Map<String, String> properties;

    /** Default constructor for Jackson deserialization only. */
    private CustomContent() {
      this(null, null, null);
    }

    /**
     * Constructor for CustomContent.
     *
     * @param customRules the custom rules of the policy
     * @param supportedObjectTypes the set of metadata object types that the policy can be applied
     *     to
     * @param properties the additional properties of the policy
     */
    private CustomContent(
        Map<String, Object> customRules,
        Set<MetadataObject.Type> supportedObjectTypes,
        Map<String, String> properties) {
      this.customRules = customRules;
      this.supportedObjectTypes =
          supportedObjectTypes == null
              ? ImmutableSet.of()
              : ImmutableSet.copyOf(supportedObjectTypes);
      this.properties = properties;
    }

    /**
     * Returns the custom rules of the policy.
     *
     * @return a map of custom rules
     */
    public Map<String, Object> customRules() {
      return customRules;
    }

    @Override
    public Map<String, Object> rules() {
      return customRules;
    }

    @Override
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return supportedObjectTypes;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof CustomContent)) return false;
      CustomContent that = (CustomContent) o;
      return Objects.equals(customRules, that.customRules)
          && Objects.equals(properties, that.properties)
          && Objects.equals(supportedObjectTypes, that.supportedObjectTypes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(customRules, properties, supportedObjectTypes);
    }

    @Override
    public String toString() {
      return "CustomContent{"
          + "customRules="
          + customRules
          + ", properties="
          + properties
          + ", supportedObjectTypes="
          + supportedObjectTypes
          + '}';
    }
  }

  /** Built-in policy content for Iceberg compaction strategy. */
  public static class IcebergCompactionContent implements PolicyContent {
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
    public static final String MIN_DATAFILE_MSE_KEY = "minDatafileMse";
    /** Rule key for minimum delete file count threshold. */
    public static final String MIN_DELETE_FILE_NUMBER_KEY = "minDeleteFileNumber";
    /** Rule key for data file MSE score weight. */
    public static final String DATAFILE_MSE_WEIGHT_KEY = "datafileMseWeight";
    /** Rule key for delete file number score weight. */
    public static final String DELETE_FILE_NUMBER_WEIGHT_KEY = "deleteFileNumberWeight";
    /** Metric name for data file MSE. */
    public static final String DATAFILE_MSE_METRIC = "custom-datafile_mse";
    /** Metric name for delete file number. */
    public static final String DELETE_FILE_NUMBER_METRIC = "custom-delete_file_number";
    /** Default score weight for data file MSE. */
    public static final long DEFAULT_DATAFILE_MSE_WEIGHT = 1L;
    /** Default score weight for delete file number. */
    public static final long DEFAULT_DELETE_FILE_NUMBER_WEIGHT = 100L;

    private static final Pattern OPTION_KEY_PATTERN = Pattern.compile("[A-Za-z0-9._-]+");
    private static final Set<MetadataObject.Type> SUPPORTED_OBJECT_TYPES =
        ImmutableSet.of(
            MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);
    private static final String TRIGGER_EXPR =
        DATAFILE_MSE_METRIC
            + " > "
            + MIN_DATAFILE_MSE_KEY
            + " || "
            + DELETE_FILE_NUMBER_METRIC
            + " > "
            + MIN_DELETE_FILE_NUMBER_KEY;
    private static final String SCORE_EXPR =
        DATAFILE_MSE_METRIC
            + " * "
            + DATAFILE_MSE_WEIGHT_KEY
            + " / 100 + "
            + DELETE_FILE_NUMBER_METRIC
            + " * "
            + DELETE_FILE_NUMBER_WEIGHT_KEY;

    private final Long minDatafileMse;
    private final Long minDeleteFileNumber;
    private final Long datafileMseWeight;
    private final Long deleteFileNumberWeight;
    private final Map<String, String> rewriteOptions;

    /** Default constructor for Jackson deserialization only. */
    private IcebergCompactionContent() {
      this(null, null, null, null, null);
    }

    private IcebergCompactionContent(
        Long minDatafileMse,
        Long minDeleteFileNumber,
        Long datafileMseWeight,
        Long deleteFileNumberWeight,
        Map<String, String> rewriteOptions) {
      this.minDatafileMse = minDatafileMse;
      this.minDeleteFileNumber = minDeleteFileNumber;
      this.datafileMseWeight =
          datafileMseWeight == null ? DEFAULT_DATAFILE_MSE_WEIGHT : datafileMseWeight;
      this.deleteFileNumberWeight =
          deleteFileNumberWeight == null
              ? DEFAULT_DELETE_FILE_NUMBER_WEIGHT
              : deleteFileNumberWeight;
      this.rewriteOptions =
          rewriteOptions == null
              ? Collections.emptyMap()
              : Collections.unmodifiableMap(new LinkedHashMap<>(rewriteOptions));
    }

    /**
     * Returns the minimum threshold for {@value DATAFILE_MSE_METRIC}.
     *
     * @return minimum data file MSE threshold
     */
    public Long minDatafileMse() {
      return minDatafileMse;
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
     * Returns the weight used by {@value DATAFILE_MSE_METRIC} in score expression.
     *
     * @return data file MSE score weight
     */
    public Long datafileMseWeight() {
      return datafileMseWeight;
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
      rules.put(MIN_DATAFILE_MSE_KEY, minDatafileMse);
      rules.put(MIN_DELETE_FILE_NUMBER_KEY, minDeleteFileNumber);
      rules.put(DATAFILE_MSE_WEIGHT_KEY, datafileMseWeight);
      rules.put(DELETE_FILE_NUMBER_WEIGHT_KEY, deleteFileNumberWeight);
      rules.put(TRIGGER_EXPR_KEY, TRIGGER_EXPR);
      rules.put(SCORE_EXPR_KEY, SCORE_EXPR);
      rewriteOptions.forEach((key, value) -> rules.put(JOB_OPTIONS_PREFIX + key, value));
      return Collections.unmodifiableMap(rules);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      PolicyContent.super.validate();
      Preconditions.checkArgument(
          minDatafileMse != null && minDatafileMse >= 0,
          "minDatafileMse must not be null and must be >= 0");
      Preconditions.checkArgument(
          minDeleteFileNumber != null && minDeleteFileNumber >= 0,
          "minDeleteFileNumber must not be null and must be >= 0");
      Preconditions.checkArgument(
          datafileMseWeight != null && datafileMseWeight >= 0,
          "datafileMseWeight must not be null and must be >= 0");
      Preconditions.checkArgument(
          deleteFileNumberWeight != null && deleteFileNumberWeight >= 0,
          "deleteFileNumberWeight must not be null and must be >= 0");

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
                StringUtils.isNotBlank(value),
                "rewrite option '%s' must have non-empty value",
                key);
          });
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof IcebergCompactionContent)) {
        return false;
      }
      IcebergCompactionContent that = (IcebergCompactionContent) o;
      return Objects.equals(minDatafileMse, that.minDatafileMse)
          && Objects.equals(minDeleteFileNumber, that.minDeleteFileNumber)
          && Objects.equals(datafileMseWeight, that.datafileMseWeight)
          && Objects.equals(deleteFileNumberWeight, that.deleteFileNumberWeight)
          && Objects.equals(rewriteOptions, that.rewriteOptions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          minDatafileMse,
          minDeleteFileNumber,
          datafileMseWeight,
          deleteFileNumberWeight,
          rewriteOptions);
    }

    @Override
    public String toString() {
      return "IcebergCompactionContent{"
          + "minDatafileMse="
          + minDatafileMse
          + ", minDeleteFileNumber="
          + minDeleteFileNumber
          + ", datafileMseWeight="
          + datafileMseWeight
          + ", deleteFileNumberWeight="
          + deleteFileNumberWeight
          + ", rewriteOptions="
          + rewriteOptions
          + '}';
    }
  }
}
