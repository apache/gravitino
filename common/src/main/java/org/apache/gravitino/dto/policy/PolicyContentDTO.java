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
package org.apache.gravitino.dto.policy;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.policy.ColumnMaskContent;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyExecutionFields;
import org.apache.gravitino.policy.RowFilterContent;

/** Represents a Policy Content Data Transfer Object (DTO). */
public interface PolicyContentDTO extends PolicyContent {

  /**
   * Optional maintenance execution peers on Iceberg TMS policy content DTOs.
   *
   * <p>Option maps always return unmodifiable defensive copies so callers cannot mutate DTO state
   * after validation or change Lombok equality/hash state through the returned map.
   */
  interface ExecutionPeersDTO {

    /**
     * Optional cron expression for automated evaluation.
     *
     * @return cron string, or {@code null} when unset
     */
    @Nullable
    String timeTrigger();

    /**
     * Optional flag to evaluate the policy after an Iceberg commit.
     *
     * @return {@code true} when enabled; {@code null} or {@code false} when unset or disabled
     */
    @Nullable
    Boolean commitTrigger();

    /**
     * Optional cooldown between finished automated runs.
     *
     * @return milliseconds, or {@code null} when unset
     */
    @Nullable
    Long minIntervalMs();

    /**
     * Optional non-secret job / client options.
     *
     * @return unmodifiable defensive copy; never {@code null} (empty when unset)
     */
    Map<String, String> jobOptions();

    /**
     * Optional non-secret updater / control-plane options.
     *
     * @return unmodifiable defensive copy; never {@code null} (empty when unset)
     */
    Map<String, String> updateOptions();
  }

  /** Represents a custom policy content DTO. */
  @EqualsAndHashCode
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class CustomContentDTO implements PolicyContentDTO {

    @JsonProperty("customRules")
    private Map<String, Object> customRules;

    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonProperty("supportedObjectTypes")
    private Set<MetadataObject.Type> supportedObjectTypes;

    // Default constructor for Jackson deserialization only.
    private CustomContentDTO() {}

    /**
     * Returns the custom rules defined in this policy content.
     *
     * @return a map of custom rules where the key is the rule name and the value is the rule value.
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
  }

  /** Represents a typed iceberg compaction policy content DTO. */
  @EqualsAndHashCode
  @ToString(exclude = {"jobOptions", "updateOptions", "rewriteOptions"})
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergCompactionContentDTO implements PolicyContentDTO, ExecutionPeersDTO {

    @JsonProperty("minDataFileMse")
    private Long minDataFileMse;

    @JsonProperty("minDeleteFileNumber")
    private Long minDeleteFileNumber;

    @JsonProperty("dataFileMseWeight")
    private Long dataFileMseWeight;

    @JsonProperty("deleteFileNumberWeight")
    private Long deleteFileNumberWeight;

    @JsonProperty("maxPartitionNum")
    private Long maxPartitionNum;

    @JsonProperty("rewriteStrategy")
    private String rewriteStrategy;

    @JsonProperty("sortOrder")
    private String sortOrder;

    @JsonProperty("rewriteOptions")
    private Map<String, String> rewriteOptions;

    @JsonProperty("timeTrigger")
    @Nullable
    private String timeTrigger;

    @JsonProperty("commitTrigger")
    @Nullable
    private Boolean commitTrigger;

    @JsonProperty("minIntervalMs")
    @Nullable
    private Long minIntervalMs;

    @JsonProperty("jobOptions")
    @Nullable
    private Map<String, String> jobOptions;

    @JsonProperty("updateOptions")
    @Nullable
    private Map<String, String> updateOptions;

    // Default constructor for Jackson deserialization only.
    private IcebergCompactionContentDTO() {}

    /**
     * Returns the minimum threshold for custom-data-file-mse metric.
     *
     * @return minimum data file MSE threshold
     */
    public Long minDataFileMse() {
      return minDataFileMse == null
          ? IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE
          : minDataFileMse;
    }

    /**
     * Returns the minimum threshold for custom-delete-file-number metric.
     *
     * @return minimum delete file number threshold
     */
    public Long minDeleteFileNumber() {
      return minDeleteFileNumber == null
          ? IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER
          : minDeleteFileNumber;
    }

    /**
     * Returns the weight for custom-data-file-mse metric in score expression.
     *
     * @return data file MSE score weight
     */
    public Long dataFileMseWeight() {
      return dataFileMseWeight == null
          ? IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT
          : dataFileMseWeight;
    }

    /**
     * Returns the weight for custom-delete-file-number metric in score expression.
     *
     * @return delete file number score weight
     */
    public Long deleteFileNumberWeight() {
      return deleteFileNumberWeight == null
          ? IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT
          : deleteFileNumberWeight;
    }

    /**
     * Returns max partition number selected for compaction.
     *
     * @return max partition number
     */
    public Long maxPartitionNum() {
      return maxPartitionNum == null
          ? IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM
          : maxPartitionNum;
    }

    /**
     * Returns the Iceberg rewrite_data_files top-level strategy.
     *
     * @return rewrite strategy
     */
    public String rewriteStrategy() {
      return rewriteStrategy == null
          ? IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY
          : rewriteStrategy;
    }

    /**
     * Returns the Iceberg rewrite_data_files top-level sort_order.
     *
     * @return sort order expression
     */
    public String sortOrder() {
      return sortOrder == null ? IcebergDataCompactionContent.DEFAULT_SORT_ORDER : sortOrder;
    }

    /**
     * Returns rewrite options expanded to job.options.* during rule generation.
     *
     * @return rewrite options map
     */
    public Map<String, String> rewriteOptions() {
      return rewriteOptions == null
          ? IcebergDataCompactionContent.DEFAULT_REWRITE_OPTIONS
          : Collections.unmodifiableMap(new LinkedHashMap<>(rewriteOptions));
    }

    @Override
    @Nullable
    public String timeTrigger() {
      return timeTrigger;
    }

    @Override
    @Nullable
    public Boolean commitTrigger() {
      return commitTrigger;
    }

    @Override
    @Nullable
    public Long minIntervalMs() {
      return minIntervalMs;
    }

    @Override
    public Map<String, String> jobOptions() {
      return PolicyExecutionFields.copyOptions(jobOptions);
    }

    @Override
    public Map<String, String> updateOptions() {
      return PolicyExecutionFields.copyOptions(updateOptions);
    }

    @Override
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return toDomainContent().supportedObjectTypes();
    }

    @Override
    public Map<String, String> properties() {
      return toDomainContent().properties();
    }

    @Override
    public Map<String, Object> rules() {
      return toDomainContent().rules();
    }

    @Override
    public void validate() throws IllegalArgumentException {
      PolicyContentDTO.super.validate();
      toDomainContent().validate();
    }

    /**
     * @return domain policy content including execution peers
     */
    public PolicyContent toDomainContent() {
      return PolicyContents.icebergDataCompaction(
          minDataFileMse(),
          minDeleteFileNumber(),
          dataFileMseWeight(),
          deleteFileNumberWeight(),
          maxPartitionNum(),
          rewriteStrategy(),
          sortOrder(),
          rewriteOptions(),
          timeTrigger(),
          commitTrigger(),
          minIntervalMs(),
          jobOptions(),
          updateOptions());
    }
  }

  /** Typed orphan cleanup policy content for REST requests and responses. */
  @EqualsAndHashCode
  @ToString(exclude = {"jobOptions", "updateOptions"})
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergOrphanFileRemovalContentDTO implements PolicyContentDTO, ExecutionPeersDTO {
    @JsonProperty("olderThanDays")
    private Long olderThanDays;

    @JsonProperty("location")
    private String location;

    @JsonProperty("dryRun")
    private Boolean dryRun;

    @JsonProperty("timeTrigger")
    @Nullable
    private String timeTrigger;

    @JsonProperty("commitTrigger")
    @Nullable
    private Boolean commitTrigger;

    @JsonProperty("minIntervalMs")
    @Nullable
    private Long minIntervalMs;

    @JsonProperty("jobOptions")
    @Nullable
    private Map<String, String> jobOptions;

    @JsonProperty("updateOptions")
    @Nullable
    private Map<String, String> updateOptions;

    private IcebergOrphanFileRemovalContentDTO() {}

    /**
     * @return minimum age in days, defaulting to three
     */
    public long olderThanDays() {
      return olderThanDays == null
          ? IcebergOrphanFileRemovalContent.DEFAULT_OLDER_THAN_DAYS
          : olderThanDays;
    }

    /**
     * @return optional scan location
     */
    @Nullable
    public String location() {
      return location;
    }

    /**
     * @return whether to list candidates without deleting them
     */
    public boolean dryRun() {
      return dryRun == null ? IcebergOrphanFileRemovalContent.DEFAULT_DRY_RUN : dryRun;
    }

    @Override
    @Nullable
    public String timeTrigger() {
      return timeTrigger;
    }

    @Override
    @Nullable
    public Boolean commitTrigger() {
      return commitTrigger;
    }

    @Override
    @Nullable
    public Long minIntervalMs() {
      return minIntervalMs;
    }

    @Override
    public Map<String, String> jobOptions() {
      return PolicyExecutionFields.copyOptions(jobOptions);
    }

    @Override
    public Map<String, String> updateOptions() {
      return PolicyExecutionFields.copyOptions(updateOptions);
    }

    @Override
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return toDomainContent().supportedObjectTypes();
    }

    @Override
    public Map<String, String> properties() {
      return toDomainContent().properties();
    }

    @Override
    public Map<String, Object> rules() {
      return toDomainContent().rules();
    }

    @Override
    public void validate() {
      Preconditions.checkArgument(
          !Boolean.TRUE.equals(commitTrigger),
          "commitTrigger is not allowed for orphan file removal");
      PolicyContentDTO.super.validate();
      toDomainContent().validate();
    }

    /**
     * @return domain policy content including execution peers
     */
    public PolicyContent toDomainContent() {
      return PolicyContents.icebergOrphanFileRemoval(
          olderThanDays(),
          location(),
          dryRun(),
          timeTrigger(),
          commitTrigger(),
          minIntervalMs(),
          jobOptions(),
          updateOptions());
    }
  }

  /** Placeholder DTO for Iceberg rewrite-manifests policy content. */
  @EqualsAndHashCode
  @ToString(exclude = {"jobOptions", "updateOptions"})
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergRewriteManifestsContentDTO implements PolicyContentDTO, ExecutionPeersDTO {
    @JsonProperty("timeTrigger")
    @Nullable
    private String timeTrigger;

    @JsonProperty("commitTrigger")
    @Nullable
    private Boolean commitTrigger;

    @JsonProperty("minIntervalMs")
    @Nullable
    private Long minIntervalMs;

    @JsonProperty("jobOptions")
    @Nullable
    private Map<String, String> jobOptions;

    @JsonProperty("updateOptions")
    @Nullable
    private Map<String, String> updateOptions;

    private IcebergRewriteManifestsContentDTO() {}

    /**
     * @return empty rewrite-manifests content DTO
     */
    public static IcebergRewriteManifestsContentDTO instance() {
      return new IcebergRewriteManifestsContentDTO();
    }

    @Override
    @Nullable
    public String timeTrigger() {
      return timeTrigger;
    }

    @Override
    @Nullable
    public Boolean commitTrigger() {
      return commitTrigger;
    }

    @Override
    @Nullable
    public Long minIntervalMs() {
      return minIntervalMs;
    }

    @Override
    public Map<String, String> jobOptions() {
      return PolicyExecutionFields.copyOptions(jobOptions);
    }

    @Override
    public Map<String, String> updateOptions() {
      return PolicyExecutionFields.copyOptions(updateOptions);
    }

    @Override
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return toDomainContent().supportedObjectTypes();
    }

    @Override
    public Map<String, String> properties() {
      return toDomainContent().properties();
    }

    @Override
    public Map<String, Object> rules() {
      return toDomainContent().rules();
    }

    @Override
    public void validate() {
      PolicyContentDTO.super.validate();
      toDomainContent().validate();
    }

    /**
     * @return domain policy content including execution peers
     */
    public PolicyContent toDomainContent() {
      return PolicyContents.icebergRewriteManifests(
          timeTrigger(), commitTrigger(), minIntervalMs(), jobOptions(), updateOptions());
    }
  }

  /** Placeholder DTO for Iceberg snapshot-expiration policy content. */
  @EqualsAndHashCode
  @ToString(exclude = {"jobOptions", "updateOptions"})
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergSnapshotExpirationContentDTO implements PolicyContentDTO, ExecutionPeersDTO {
    @JsonProperty("timeTrigger")
    @Nullable
    private String timeTrigger;

    @JsonProperty("commitTrigger")
    @Nullable
    private Boolean commitTrigger;

    @JsonProperty("minIntervalMs")
    @Nullable
    private Long minIntervalMs;

    @JsonProperty("jobOptions")
    @Nullable
    private Map<String, String> jobOptions;

    @JsonProperty("updateOptions")
    @Nullable
    private Map<String, String> updateOptions;

    private IcebergSnapshotExpirationContentDTO() {}

    /**
     * @return empty snapshot-expiration content DTO
     */
    public static IcebergSnapshotExpirationContentDTO instance() {
      return new IcebergSnapshotExpirationContentDTO();
    }

    @Override
    @Nullable
    public String timeTrigger() {
      return timeTrigger;
    }

    @Override
    @Nullable
    public Boolean commitTrigger() {
      return commitTrigger;
    }

    @Override
    @Nullable
    public Long minIntervalMs() {
      return minIntervalMs;
    }

    @Override
    public Map<String, String> jobOptions() {
      return PolicyExecutionFields.copyOptions(jobOptions);
    }

    @Override
    public Map<String, String> updateOptions() {
      return PolicyExecutionFields.copyOptions(updateOptions);
    }

    @Override
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return toDomainContent().supportedObjectTypes();
    }

    @Override
    public Map<String, String> properties() {
      return toDomainContent().properties();
    }

    @Override
    public Map<String, Object> rules() {
      return toDomainContent().rules();
    }

    @Override
    public void validate() {
      PolicyContentDTO.super.validate();
      toDomainContent().validate();
    }

    /**
     * @return domain policy content including execution peers
     */
    public PolicyContent toDomainContent() {
      return PolicyContents.icebergSnapshotExpiration(
          timeTrigger(), commitTrigger(), minIntervalMs(), jobOptions(), updateOptions());
    }
  }

  /** Represents typed row-filter policy content. */
  @EqualsAndHashCode
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class RowFilterContentDTO implements PolicyContentDTO {

    @JsonProperty("expression")
    @Nullable
    private String expression;

    // Default constructor for Jackson deserialization only.
    private RowFilterContentDTO() {}

    /**
     * Returns the authored row-filter expression.
     *
     * @return row-filter expression
     */
    @Nullable
    public String expression() {
      return expression;
    }

    @Override
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return toDomainContent().supportedObjectTypes();
    }

    @Override
    public Map<String, String> properties() {
      return toDomainContent().properties();
    }

    @Override
    public Map<String, Object> rules() {
      return toDomainContent().rules();
    }

    @Override
    public void validate() throws IllegalArgumentException {
      PolicyContentDTO.super.validate();
      toDomainContent().validate();
    }

    private RowFilterContent toDomainContent() {
      return PolicyContents.rowFilter(expression);
    }
  }

  /** Represents typed column-mask policy content. */
  @EqualsAndHashCode
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class ColumnMaskContentDTO implements PolicyContentDTO {

    @JsonProperty("expression")
    @Nullable
    private String expression;

    // Default constructor for Jackson deserialization only.
    private ColumnMaskContentDTO() {}

    /**
     * Returns the authored column-mask expression.
     *
     * @return column-mask expression
     */
    @Nullable
    public String expression() {
      return expression;
    }

    @Override
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return toDomainContent().supportedObjectTypes();
    }

    @Override
    public Map<String, String> properties() {
      return toDomainContent().properties();
    }

    @Override
    public Map<String, Object> rules() {
      return toDomainContent().rules();
    }

    @Override
    public void validate() throws IllegalArgumentException {
      PolicyContentDTO.super.validate();
      toDomainContent().validate();
    }

    private ColumnMaskContent toDomainContent() {
      return PolicyContents.columnMask(expression);
    }
  }
}
