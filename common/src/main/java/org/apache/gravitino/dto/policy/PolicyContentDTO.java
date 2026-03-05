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
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
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

    @JsonProperty("minDatafileMse")
    private Long minDatafileMse;

    @JsonProperty("minDeleteFileNumber")
    private Long minDeleteFileNumber;

    @JsonProperty("datafileMseWeight")
    private Long datafileMseWeight;

    @JsonProperty("deleteFileNumberWeight")
    private Long deleteFileNumberWeight;

    @JsonProperty("rewriteOptions")
    private Map<String, String> rewriteOptions;

    private static final Pattern OPTION_KEY_PATTERN = Pattern.compile("[A-Za-z0-9._-]+");

    // Default constructor for Jackson deserialization only.
    private IcebergCompactionContentDTO() {}

    /**
     * Returns the minimum threshold for custom-datafile_mse metric.
     *
     * @return minimum data file MSE threshold
     */
    public Long minDatafileMse() {
      return minDatafileMse;
    }

    /**
     * Returns the minimum threshold for custom-delete_file_number metric.
     *
     * @return minimum delete file number threshold
     */
    public Long minDeleteFileNumber() {
      return minDeleteFileNumber;
    }

    /**
     * Returns the weight for custom-datafile_mse metric in score expression.
     *
     * @return data file MSE score weight
     */
    public Long datafileMseWeight() {
      return datafileMseWeight == null
          ? PolicyContents.IcebergCompactionContent.DEFAULT_DATAFILE_MSE_WEIGHT
          : datafileMseWeight;
    }

    /**
     * Returns the weight for custom-delete_file_number metric in score expression.
     *
     * @return delete file number score weight
     */
    public Long deleteFileNumberWeight() {
      return deleteFileNumberWeight == null
          ? PolicyContents.IcebergCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT
          : deleteFileNumberWeight;
    }

    /**
     * Returns rewrite options expanded to job.options.* during rule generation.
     *
     * @return rewrite options map
     */
    public Map<String, String> rewriteOptions() {
      return rewriteOptions == null
          ? Map.of()
          : Collections.unmodifiableMap(new LinkedHashMap<>(rewriteOptions));
    }

    @Override
    public Set<MetadataObject.Type> supportedObjectTypes() {
      return ImmutableSet.of(
          MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);
    }

    @Override
    public Map<String, String> properties() {
      return PolicyContents.icebergCompaction(
              require(minDatafileMse, "minDatafileMse"),
              require(minDeleteFileNumber, "minDeleteFileNumber"),
              datafileMseWeight(),
              deleteFileNumberWeight(),
              rewriteOptions())
          .properties();
    }

    @Override
    public Map<String, Object> rules() {
      return PolicyContents.icebergCompaction(
              require(minDatafileMse, "minDatafileMse"),
              require(minDeleteFileNumber, "minDeleteFileNumber"),
              datafileMseWeight(),
              deleteFileNumberWeight(),
              rewriteOptions())
          .rules();
    }

    @Override
    public void validate() throws IllegalArgumentException {
      PolicyContentDTO.super.validate();
      Preconditions.checkArgument(
          minDatafileMse != null && minDatafileMse >= 0,
          "minDatafileMse must not be null and must be >= 0");
      Preconditions.checkArgument(
          minDeleteFileNumber != null && minDeleteFileNumber >= 0,
          "minDeleteFileNumber must not be null and must be >= 0");
      Preconditions.checkArgument(datafileMseWeight() >= 0, "datafileMseWeight must be >= 0");
      Preconditions.checkArgument(
          deleteFileNumberWeight() >= 0, "deleteFileNumberWeight must be >= 0");
      rewriteOptions()
          .forEach(
              (key, value) -> {
                Preconditions.checkArgument(
                    StringUtils.isNotBlank(key), "rewrite option key is blank");
                Preconditions.checkArgument(
                    OPTION_KEY_PATTERN.matcher(key).matches(),
                    "rewrite option key '%s' contains illegal characters",
                    key);
                Preconditions.checkArgument(
                    !key.startsWith(PolicyContents.IcebergCompactionContent.JOB_OPTIONS_PREFIX),
                    "rewrite option key '%s' must not start with '%s'",
                    key,
                    PolicyContents.IcebergCompactionContent.JOB_OPTIONS_PREFIX);
                Preconditions.checkArgument(
                    StringUtils.isNotBlank(value),
                    "rewrite option '%s' must have non-empty value",
                    key);
              });
    }

    private static long require(Long value, String fieldName) {
      Preconditions.checkArgument(value != null, "%s must not be null", fieldName);
      return value;
    }
  }
}
