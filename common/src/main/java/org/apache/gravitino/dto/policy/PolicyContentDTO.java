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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
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
import org.apache.gravitino.policy.PolicyExecutionSpec;
import org.apache.gravitino.policy.RowFilterContent;

/** Represents a Policy Content Data Transfer Object (DTO). */
public interface PolicyContentDTO extends PolicyContent {

  /**
   * Optional maintenance execution peers on Iceberg TMS policy content DTOs.
   *
   * <p>Peers are stored as a composed {@link PolicyExecutionSpec}. Flat JSON keys are exposed via
   * {@code @JsonProperty} accessors so the wire contract stays flat without duplicating five fields
   * on every DTO.
   *
   * <p>Option maps always return unmodifiable defensive copies so callers cannot mutate DTO state
   * after validation or change Lombok equality/hash state through the returned map.
   */
  interface ExecutionPeersDTO {

    /**
     * Stores {@code null} when every peer is unset so Lombok equality matches Jackson round-trips.
     *
     * @param spec peers to store; empty specs are normalized to {@code null}
     * @return {@code null} when unset, otherwise the spec
     */
    static PolicyExecutionSpec normalize(@Nullable PolicyExecutionSpec spec) {
      return spec == null || !spec.hasAny() ? null : spec;
    }

    /**
     * @return immutable execution peers; never {@code null}
     */
    @JsonIgnore
    PolicyExecutionSpec executionSpec();

    /**
     * Optional cron expression for automated evaluation.
     *
     * @return cron string, or {@code null} when unset
     */
    @Nullable
    @JsonProperty(PolicyExecutionFields.TIME_TRIGGER_JSON_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    default String timeTrigger() {
      return executionSpec().timeTrigger();
    }

    /**
     * Optional flag to evaluate the policy after an Iceberg commit.
     *
     * @return {@code true} when enabled, {@code false} when explicitly disabled, or {@code null}
     *     when unset
     */
    @Nullable
    @JsonProperty(PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    default Boolean commitTrigger() {
      return executionSpec().commitTrigger();
    }

    /**
     * Optional cooldown between finished automated runs.
     *
     * @return milliseconds, or {@code null} when unset
     */
    @Nullable
    @JsonProperty(PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    default Long minIntervalMs() {
      return executionSpec().minIntervalMs();
    }

    /**
     * Optional non-secret job / client options.
     *
     * @return unmodifiable defensive copy; never {@code null} (empty when unset)
     */
    @JsonProperty(PolicyExecutionFields.JOB_OPTIONS_JSON_KEY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    default Map<String, String> jobOptions() {
      return executionSpec().jobOptions();
    }

    /**
     * Optional non-secret updater / control-plane options.
     *
     * @return unmodifiable defensive copy; never {@code null} (empty when unset)
     */
    @JsonProperty(PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    default Map<String, String> updaterOptions() {
      return executionSpec().updaterOptions();
    }
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
  @ToString(exclude = {"executionSpec", "rewriteOptions"})
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

    @EqualsAndHashCode.Exclude @JsonIgnore @Nullable private PolicyExecutionSpec executionSpec;

    @EqualsAndHashCode.Include
    private PolicyExecutionSpec executionSpecForEquals() {
      return ExecutionPeersDTO.normalize(executionSpec);
    }

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
    @JsonIgnore
    public PolicyExecutionSpec executionSpec() {
      return executionSpec == null ? PolicyExecutionSpec.empty() : executionSpec;
    }

    @JsonProperty(PolicyExecutionFields.TIME_TRIGGER_JSON_KEY)
    private void setTimeTrigger(String timeTrigger) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withTimeTrigger(timeTrigger));
    }

    @JsonProperty(PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY)
    private void setCommitTrigger(Boolean commitTrigger) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withCommitTrigger(commitTrigger));
    }

    @JsonProperty(PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY)
    private void setMinIntervalMs(Long minIntervalMs) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withMinIntervalMs(minIntervalMs));
    }

    @JsonProperty(PolicyExecutionFields.JOB_OPTIONS_JSON_KEY)
    private void setJobOptions(Map<String, String> jobOptions) {
      this.executionSpec = ExecutionPeersDTO.normalize(executionSpec().withJobOptions(jobOptions));
    }

    @JsonProperty(PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY)
    private void setUpdaterOptions(Map<String, String> updaterOptions) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withUpdaterOptions(updaterOptions));
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
      PolicyExecutionSpec spec = executionSpec();
      return PolicyContents.icebergDataCompaction(
          minDataFileMse(),
          minDeleteFileNumber(),
          dataFileMseWeight(),
          deleteFileNumberWeight(),
          maxPartitionNum(),
          rewriteStrategy(),
          sortOrder(),
          rewriteOptions(),
          spec.timeTrigger(),
          spec.commitTrigger(),
          spec.minIntervalMs(),
          spec.jobOptions(),
          spec.updaterOptions());
    }
  }

  /** Typed orphan cleanup policy content for REST requests and responses. */
  @EqualsAndHashCode
  @ToString(exclude = {"executionSpec"})
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergOrphanFileRemovalContentDTO implements PolicyContentDTO, ExecutionPeersDTO {
    @JsonProperty("olderThanDays")
    private Long olderThanDays;

    @JsonProperty("location")
    private String location;

    @JsonProperty("dryRun")
    private Boolean dryRun;

    @EqualsAndHashCode.Exclude @JsonIgnore @Nullable private PolicyExecutionSpec executionSpec;

    @EqualsAndHashCode.Include
    private PolicyExecutionSpec executionSpecForEquals() {
      return ExecutionPeersDTO.normalize(executionSpec);
    }

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
    @JsonIgnore
    public PolicyExecutionSpec executionSpec() {
      return executionSpec == null ? PolicyExecutionSpec.empty() : executionSpec;
    }

    @JsonProperty(PolicyExecutionFields.TIME_TRIGGER_JSON_KEY)
    private void setTimeTrigger(String timeTrigger) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withTimeTrigger(timeTrigger));
    }

    @JsonProperty(PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY)
    private void setCommitTrigger(Boolean commitTrigger) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withCommitTrigger(commitTrigger));
    }

    @JsonProperty(PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY)
    private void setMinIntervalMs(Long minIntervalMs) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withMinIntervalMs(minIntervalMs));
    }

    @JsonProperty(PolicyExecutionFields.JOB_OPTIONS_JSON_KEY)
    private void setJobOptions(Map<String, String> jobOptions) {
      this.executionSpec = ExecutionPeersDTO.normalize(executionSpec().withJobOptions(jobOptions));
    }

    @JsonProperty(PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY)
    private void setUpdaterOptions(Map<String, String> updaterOptions) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withUpdaterOptions(updaterOptions));
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
          !Boolean.TRUE.equals(commitTrigger()),
          "commitTrigger is not allowed for orphan file removal");
      PolicyContentDTO.super.validate();
      toDomainContent().validate();
    }

    /**
     * @return domain policy content including execution peers
     */
    public PolicyContent toDomainContent() {
      PolicyExecutionSpec spec = executionSpec();
      return PolicyContents.icebergOrphanFileRemoval(
          olderThanDays(),
          location(),
          dryRun(),
          spec.timeTrigger(),
          spec.commitTrigger(),
          spec.minIntervalMs(),
          spec.jobOptions(),
          spec.updaterOptions());
    }
  }

  /** Placeholder DTO for Iceberg rewrite-manifests policy content. */
  @EqualsAndHashCode
  @ToString(exclude = {"executionSpec"})
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergRewriteManifestsContentDTO implements PolicyContentDTO, ExecutionPeersDTO {
    @EqualsAndHashCode.Exclude @JsonIgnore @Nullable private PolicyExecutionSpec executionSpec;

    @EqualsAndHashCode.Include
    private PolicyExecutionSpec executionSpecForEquals() {
      return ExecutionPeersDTO.normalize(executionSpec);
    }

    private IcebergRewriteManifestsContentDTO() {}

    /**
     * @return empty rewrite-manifests content DTO
     */
    public static IcebergRewriteManifestsContentDTO instance() {
      return new IcebergRewriteManifestsContentDTO();
    }

    @Override
    @JsonIgnore
    public PolicyExecutionSpec executionSpec() {
      return executionSpec == null ? PolicyExecutionSpec.empty() : executionSpec;
    }

    @JsonProperty(PolicyExecutionFields.TIME_TRIGGER_JSON_KEY)
    private void setTimeTrigger(String timeTrigger) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withTimeTrigger(timeTrigger));
    }

    @JsonProperty(PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY)
    private void setCommitTrigger(Boolean commitTrigger) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withCommitTrigger(commitTrigger));
    }

    @JsonProperty(PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY)
    private void setMinIntervalMs(Long minIntervalMs) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withMinIntervalMs(minIntervalMs));
    }

    @JsonProperty(PolicyExecutionFields.JOB_OPTIONS_JSON_KEY)
    private void setJobOptions(Map<String, String> jobOptions) {
      this.executionSpec = ExecutionPeersDTO.normalize(executionSpec().withJobOptions(jobOptions));
    }

    @JsonProperty(PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY)
    private void setUpdaterOptions(Map<String, String> updaterOptions) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withUpdaterOptions(updaterOptions));
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
      PolicyExecutionSpec spec = executionSpec();
      return PolicyContents.icebergRewriteManifests(
          spec.timeTrigger(),
          spec.commitTrigger(),
          spec.minIntervalMs(),
          spec.jobOptions(),
          spec.updaterOptions());
    }
  }

  /** Placeholder DTO for Iceberg snapshot-expiration policy content. */
  @EqualsAndHashCode
  @ToString(exclude = {"executionSpec"})
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergSnapshotExpirationContentDTO implements PolicyContentDTO, ExecutionPeersDTO {
    @EqualsAndHashCode.Exclude @JsonIgnore @Nullable private PolicyExecutionSpec executionSpec;

    @EqualsAndHashCode.Include
    private PolicyExecutionSpec executionSpecForEquals() {
      return ExecutionPeersDTO.normalize(executionSpec);
    }

    private IcebergSnapshotExpirationContentDTO() {}

    /**
     * @return empty snapshot-expiration content DTO
     */
    public static IcebergSnapshotExpirationContentDTO instance() {
      return new IcebergSnapshotExpirationContentDTO();
    }

    @Override
    @JsonIgnore
    public PolicyExecutionSpec executionSpec() {
      return executionSpec == null ? PolicyExecutionSpec.empty() : executionSpec;
    }

    @JsonProperty(PolicyExecutionFields.TIME_TRIGGER_JSON_KEY)
    private void setTimeTrigger(String timeTrigger) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withTimeTrigger(timeTrigger));
    }

    @JsonProperty(PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY)
    private void setCommitTrigger(Boolean commitTrigger) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withCommitTrigger(commitTrigger));
    }

    @JsonProperty(PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY)
    private void setMinIntervalMs(Long minIntervalMs) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withMinIntervalMs(minIntervalMs));
    }

    @JsonProperty(PolicyExecutionFields.JOB_OPTIONS_JSON_KEY)
    private void setJobOptions(Map<String, String> jobOptions) {
      this.executionSpec = ExecutionPeersDTO.normalize(executionSpec().withJobOptions(jobOptions));
    }

    @JsonProperty(PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY)
    private void setUpdaterOptions(Map<String, String> updaterOptions) {
      this.executionSpec =
          ExecutionPeersDTO.normalize(executionSpec().withUpdaterOptions(updaterOptions));
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
      PolicyExecutionSpec spec = executionSpec();
      return PolicyContents.icebergSnapshotExpiration(
          spec.timeTrigger(),
          spec.commitTrigger(),
          spec.minIntervalMs(),
          spec.jobOptions(),
          spec.updaterOptions());
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
