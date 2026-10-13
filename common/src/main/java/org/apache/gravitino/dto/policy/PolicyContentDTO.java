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
import org.apache.gravitino.maintenance.policy.MaintenanceTrigger;
import org.apache.gravitino.maintenance.policy.TableMaintenancePolicies;
import org.apache.gravitino.policy.ColumnMaskContent;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.RowFilterContent;

/** Represents a Policy Content Data Transfer Object (DTO). */
public interface PolicyContentDTO extends PolicyContent {

  /**
   * Validates optional TMS fields on a maintenance policy content DTO.
   *
   * @param policyType built-in policy type
   * @param schedule schedule DTO
   * @param minIntervalMs minimum interval ms
   * @param jobOptions job options map
   * @param updateOptions update options map
   */
  static void validateMaintenanceFields(
      Policy.BuiltInType policyType,
      @Nullable MaintenanceTriggerDTO schedule,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    TableMaintenancePolicies.validate(
        policyType,
        schedule == null ? null : schedule.toDomain(),
        minIntervalMs,
        jobOptions == null ? Collections.emptyMap() : jobOptions,
        updateOptions == null ? Collections.emptyMap() : updateOptions);
  }

  /** JSON DTO for {@link MaintenanceTrigger} (serialized as content {@code schedule}). */
  @EqualsAndHashCode
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class MaintenanceTriggerDTO {
    @JsonProperty("onCommit")
    private Boolean onCommit;

    @JsonProperty("cronExpression")
    private String cronExpression;

    private MaintenanceTriggerDTO() {}

    /**
     * Converts to domain trigger.
     *
     * @return domain object, or {@code null} when unset
     */
    @Nullable
    public MaintenanceTrigger toDomain() {
      if (onCommit == null && cronExpression == null) {
        return null;
      }
      return new MaintenanceTrigger(onCommit, cronExpression);
    }

    /**
     * Creates a DTO from a domain trigger.
     *
     * @param trigger domain trigger
     * @return DTO, or {@code null} when trigger is null
     */
    @Nullable
    public static MaintenanceTriggerDTO fromDomain(@Nullable MaintenanceTrigger trigger) {
      if (trigger == null) {
        return null;
      }
      return MaintenanceTriggerDTO.builder()
          .withOnCommit(trigger.onCommit())
          .withCronExpression(trigger.cronExpression())
          .build();
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
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergCompactionContentDTO implements PolicyContentDTO {

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

    @JsonProperty("schedule")
    @Nullable
    private MaintenanceTriggerDTO schedule;

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

    /**
     * @return schedule DTO, or null
     */
    @Nullable
    public MaintenanceTriggerDTO schedule() {
      return schedule;
    }

    /**
     * @return min interval ms, or null
     */
    @Nullable
    public Long minIntervalMs() {
      return minIntervalMs;
    }

    /**
     * @return job options, or null
     */
    @Nullable
    public Map<String, String> jobOptions() {
      return jobOptions;
    }

    /**
     * @return update options, or null
     */
    @Nullable
    public Map<String, String> updateOptions() {
      return updateOptions;
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
      PolicyContent content = toDomainContent();
      content.validate();
      TableMaintenancePolicies.validate(Policy.BuiltInType.ICEBERG_COMPACTION, content);
    }

    /**
     * @return domain policy content including TMS peer fields
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
          schedule == null ? null : schedule.toDomain(),
          minIntervalMs,
          jobOptions,
          updateOptions);
    }
  }

  /** Typed orphan cleanup policy content for REST requests and responses. */
  @EqualsAndHashCode
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergOrphanFileRemovalContentDTO implements PolicyContentDTO {
    @JsonProperty("olderThanDays")
    private Long olderThanDays;

    @JsonProperty("location")
    private String location;

    @JsonProperty("dryRun")
    private Boolean dryRun;

    @JsonProperty("schedule")
    @Nullable
    private MaintenanceTriggerDTO schedule;

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

    /**
     * @return schedule DTO, or null
     */
    @Nullable
    public MaintenanceTriggerDTO schedule() {
      return schedule;
    }

    /**
     * @return min interval ms, or null
     */
    @Nullable
    public Long minIntervalMs() {
      return minIntervalMs;
    }

    /**
     * @return job options, or null
     */
    @Nullable
    public Map<String, String> jobOptions() {
      return jobOptions;
    }

    /**
     * @return update options, or null
     */
    @Nullable
    public Map<String, String> updateOptions() {
      return updateOptions;
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
      PolicyContent content = toDomainContent();
      content.validate();
      TableMaintenancePolicies.validate(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL, content);
    }

    /**
     * @return domain policy content including TMS peer fields
     */
    public PolicyContent toDomainContent() {
      return PolicyContents.icebergOrphanFileRemoval(
          olderThanDays(),
          location(),
          dryRun(),
          schedule == null ? null : schedule.toDomain(),
          minIntervalMs,
          jobOptions,
          updateOptions);
    }
  }

  /** Placeholder DTO for Iceberg rewrite-manifests policy content. */
  @EqualsAndHashCode
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergRewriteManifestsContentDTO implements PolicyContentDTO {
    @JsonProperty("schedule")
    @Nullable
    private MaintenanceTriggerDTO schedule;

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

    /**
     * @return schedule DTO, or null
     */
    @Nullable
    public MaintenanceTriggerDTO schedule() {
      return schedule;
    }

    /**
     * @return min interval ms, or null
     */
    @Nullable
    public Long minIntervalMs() {
      return minIntervalMs;
    }

    /**
     * @return job options, or null
     */
    @Nullable
    public Map<String, String> jobOptions() {
      return jobOptions;
    }

    /**
     * @return update options, or null
     */
    @Nullable
    public Map<String, String> updateOptions() {
      return updateOptions;
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
      PolicyContent content = toDomainContent();
      content.validate();
      TableMaintenancePolicies.validate(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, content);
    }

    /**
     * @return domain policy content including TMS peer fields
     */
    public PolicyContent toDomainContent() {
      return PolicyContents.icebergRewriteManifests(
          schedule == null ? null : schedule.toDomain(), minIntervalMs, jobOptions, updateOptions);
    }
  }

  /** Placeholder DTO for Iceberg snapshot-expiration policy content. */
  @EqualsAndHashCode
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergSnapshotExpirationContentDTO implements PolicyContentDTO {
    @JsonProperty("schedule")
    @Nullable
    private MaintenanceTriggerDTO schedule;

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

    /**
     * @return schedule DTO, or null
     */
    @Nullable
    public MaintenanceTriggerDTO schedule() {
      return schedule;
    }

    /**
     * @return min interval ms, or null
     */
    @Nullable
    public Long minIntervalMs() {
      return minIntervalMs;
    }

    /**
     * @return job options, or null
     */
    @Nullable
    public Map<String, String> jobOptions() {
      return jobOptions;
    }

    /**
     * @return update options, or null
     */
    @Nullable
    public Map<String, String> updateOptions() {
      return updateOptions;
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
      PolicyContent content = toDomainContent();
      content.validate();
      TableMaintenancePolicies.validate(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, content);
    }

    /**
     * @return domain policy content including TMS peer fields
     */
    public PolicyContent toDomainContent() {
      return PolicyContents.icebergSnapshotExpiration(
          schedule == null ? null : schedule.toDomain(), minIntervalMs, jobOptions, updateOptions);
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
