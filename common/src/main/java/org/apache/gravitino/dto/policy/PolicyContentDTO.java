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
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;

/** Represents a Policy Content Data Transfer Object (DTO). */
public interface PolicyContentDTO extends PolicyContent {

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

    @JsonProperty("rewriteOptions")
    private Map<String, String> rewriteOptions;

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

    private PolicyContent toDomainContent() {
      return PolicyContents.icebergDataCompaction(
          minDataFileMse(),
          minDeleteFileNumber(),
          dataFileMseWeight(),
          deleteFileNumberWeight(),
          maxPartitionNum(),
          rewriteOptions());
    }
  }

  /** Typed manifest rewrite policy content. */
  @EqualsAndHashCode
  @ToString
  @Builder(setterPrefix = "with")
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  class IcebergRewriteManifestsContentDTO implements PolicyContentDTO {
    @JsonProperty("manifest_count_critical")
    private Long manifestCountCritical;

    @JsonProperty("manifest_count_warning")
    private Long manifestCountWarning;

    @JsonProperty("avg_manifest_size_threshold_bytes")
    private Long avgManifestSizeThresholdBytes;

    @JsonProperty("spec_id")
    private Integer specId;

    @JsonProperty("use_caching")
    private Boolean useCaching;

    private IcebergRewriteManifestsContentDTO() {}

    /**
     * Returns manifest_count_critical.
     *
     * @return manifest_count_critical
     */
    public Long manifestCountCritical() {
      return manifestCountCritical == null
          ? IcebergRewriteManifestsContent.DEFAULT_MANIFEST_COUNT_CRITICAL
          : manifestCountCritical;
    }

    /**
     * Returns manifest_count_warning.
     *
     * @return manifest_count_warning
     */
    public Long manifestCountWarning() {
      return manifestCountWarning == null
          ? IcebergRewriteManifestsContent.DEFAULT_MANIFEST_COUNT_WARNING
          : manifestCountWarning;
    }

    /**
     * Returns avg_manifest_size_threshold_bytes.
     *
     * @return avg_manifest_size_threshold_bytes
     */
    public Long avgManifestSizeThresholdBytes() {
      return avgManifestSizeThresholdBytes == null
          ? IcebergRewriteManifestsContent.DEFAULT_AVG_MANIFEST_SIZE_THRESHOLD_BYTES
          : avgManifestSizeThresholdBytes;
    }

    /**
     * Returns spec_id.
     *
     * @return spec_id or null when omitted
     */
    @Nullable
    public Integer specId() {
      return specId;
    }

    /**
     * Returns use_caching.
     *
     * @return use_caching or null when omitted
     */
    @Nullable
    public Boolean useCaching() {
      return useCaching;
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
      toDomainContent().validate();
    }

    private PolicyContent toDomainContent() {
      return PolicyContents.icebergRewriteManifests(
          manifestCountCritical(),
          manifestCountWarning(),
          avgManifestSizeThresholdBytes(),
          specId(),
          useCaching());
    }
  }
}
