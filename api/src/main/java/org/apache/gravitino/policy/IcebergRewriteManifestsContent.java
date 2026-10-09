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
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;

/** Typed thresholds and target options for Iceberg manifest rewriting. */
public final class IcebergRewriteManifestsContent implements PolicyContent {
  /** Strategy type for manifest rewriting. */
  public static final String STRATEGY_TYPE_VALUE = "iceberg-rewrite-manifests";
  /** Built-in rewrite job template. */
  public static final String JOB_TEMPLATE_NAME_VALUE = "builtin-iceberg-rewrite-manifests";
  /** Rule key for critical manifest count. */
  public static final String MANIFEST_COUNT_CRITICAL = "manifest_count_critical";
  /** Default critical manifest count. */
  public static final long DEFAULT_MANIFEST_COUNT_CRITICAL = 500L;
  /** Rule key for minimum count for the size trigger. */
  public static final String MANIFEST_COUNT_WARNING = "manifest_count_warning";
  /** Default minimum count for the size trigger. */
  public static final long DEFAULT_MANIFEST_COUNT_WARNING = 100L;
  /** Rule key for exclusive average size threshold in bytes. */
  public static final String AVG_MANIFEST_SIZE_THRESHOLD_BYTES =
      "avg_manifest_size_threshold_bytes";
  /** Default exclusive average size threshold in bytes. */
  public static final long DEFAULT_AVG_MANIFEST_SIZE_THRESHOLD_BYTES = 8388608L;
  /** Rule key for requested partition spec ID, or null to use the cycle target. */
  public static final String SPEC_ID = "spec_id";
  /** Rule key for caching option, or null for the Iceberg default. */
  public static final String USE_CACHING = "use_caching";

  private final Long manifestCountCritical;
  private final Long manifestCountWarning;
  private final Long avgManifestSizeThresholdBytes;
  private final Integer specId;
  private final Boolean useCaching;

  private IcebergRewriteManifestsContent() {
    this(null, null, null, null, null);
  }

  IcebergRewriteManifestsContent(
      @Nullable Long manifestCountCritical,
      @Nullable Long manifestCountWarning,
      @Nullable Long avgManifestSizeThresholdBytes,
      @Nullable Integer specId,
      @Nullable Boolean useCaching) {
    this.manifestCountCritical =
        manifestCountCritical == null ? DEFAULT_MANIFEST_COUNT_CRITICAL : manifestCountCritical;
    this.manifestCountWarning =
        manifestCountWarning == null ? DEFAULT_MANIFEST_COUNT_WARNING : manifestCountWarning;
    this.avgManifestSizeThresholdBytes =
        avgManifestSizeThresholdBytes == null
            ? DEFAULT_AVG_MANIFEST_SIZE_THRESHOLD_BYTES
            : avgManifestSizeThresholdBytes;
    this.specId = specId;
    this.useCaching = useCaching;
  }

  /**
   * Returns the critical manifest count.
   *
   * @return critical manifest count
   */
  public Long manifestCountCritical() {
    return manifestCountCritical;
  }

  /**
   * Returns the minimum count for the size trigger.
   *
   * @return minimum count for the size trigger
   */
  public Long manifestCountWarning() {
    return manifestCountWarning;
  }

  /**
   * Returns the exclusive average size threshold in bytes.
   *
   * @return exclusive average size threshold in bytes
   */
  public Long avgManifestSizeThresholdBytes() {
    return avgManifestSizeThresholdBytes;
  }

  /**
   * Returns the requested partition spec ID, or null to use the cycle target.
   *
   * @return requested partition spec ID, or null to use the cycle target
   */
  @Nullable
  public Integer specId() {
    return specId;
  }

  /**
   * Returns the caching option, or null for the Iceberg default.
   *
   * @return caching option, or null for the Iceberg default
   */
  @Nullable
  public Boolean useCaching() {
    return useCaching;
  }

  @Override
  public Set<MetadataObject.Type> supportedObjectTypes() {
    return ImmutableSet.of(
        MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of(
        "strategy.type", STRATEGY_TYPE_VALUE, "job.template-name", JOB_TEMPLATE_NAME_VALUE);
  }

  @Override
  public Map<String, Object> rules() {
    Map<String, Object> rules = new LinkedHashMap<>();
    rules.put(MANIFEST_COUNT_CRITICAL, manifestCountCritical);
    rules.put(MANIFEST_COUNT_WARNING, manifestCountWarning);
    rules.put(AVG_MANIFEST_SIZE_THRESHOLD_BYTES, avgManifestSizeThresholdBytes);
    if (specId != null) {
      rules.put(SPEC_ID, specId);
    }
    if (useCaching != null) {
      rules.put(USE_CACHING, useCaching);
    }
    return Collections.unmodifiableMap(rules);
  }

  @Override
  public void validate() {
    PolicyContent.super.validate();
    Preconditions.checkArgument(manifestCountWarning > 0, "manifest_count_warning must be > 0");
    Preconditions.checkArgument(
        manifestCountCritical >= manifestCountWarning,
        "manifest_count_critical must be >= manifest_count_warning");
    Preconditions.checkArgument(
        avgManifestSizeThresholdBytes > 0, "avg_manifest_size_threshold_bytes must be > 0");
    Preconditions.checkArgument(specId == null || specId >= 0, "spec_id must be >= 0");
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IcebergRewriteManifestsContent)) {
      return false;
    }
    IcebergRewriteManifestsContent that = (IcebergRewriteManifestsContent) other;
    return Objects.equals(manifestCountCritical, that.manifestCountCritical)
        && Objects.equals(manifestCountWarning, that.manifestCountWarning)
        && Objects.equals(avgManifestSizeThresholdBytes, that.avgManifestSizeThresholdBytes)
        && Objects.equals(specId, that.specId)
        && Objects.equals(useCaching, that.useCaching);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        manifestCountCritical,
        manifestCountWarning,
        avgManifestSizeThresholdBytes,
        specId,
        useCaching);
  }
}
