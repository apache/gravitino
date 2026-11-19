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
public class IcebergRewriteManifestsContent extends AbstractExecutablePolicyContent {
  /** Property key for strategy type. */
  public static final String STRATEGY_TYPE_KEY = "strategy.type";
  /** Property key for the job template. */
  public static final String JOB_TEMPLATE_NAME_KEY = "job.template-name";
  /** Rule key for the trigger expression. */
  public static final String TRIGGER_EXPR_KEY = "trigger-expr";
  /** Strategy type for manifest rewriting. */
  public static final String STRATEGY_TYPE_VALUE = "iceberg-rewrite-manifests";
  /** Built-in rewrite job template. */
  public static final String JOB_TEMPLATE_NAME_VALUE = "builtin-iceberg-rewrite-manifests";
  /**
   * Rule key for the inclusive manifest-count trigger. Aligns with Iceberg's {@code
   * commit.manifest.min-count-to-merge} default.
   */
  public static final String MANIFEST_COUNT_THRESHOLD = "manifest_count_threshold";
  /** Default inclusive manifest-count trigger ({@code commit.manifest.min-count-to-merge}). */
  public static final long DEFAULT_MANIFEST_COUNT_THRESHOLD = 100L;
  /** S0 metric: total manifest count (TMS / in-job gate). */
  public static final String MANIFEST_NUMBER_METRIC = "custom-manifest-number";
  /** S0 metric: average manifest size in bytes (collected for observability). */
  public static final String AVG_MANIFEST_SIZE_METRIC = "custom-avg-manifest-size";
  /** S1 metric: manifests rewritten by the procedure. */
  public static final String REWRITTEN_MANIFEST_NUMBER_METRIC = "custom-rewritten-manifest-number";
  /** Rule key for the requested partition spec ID; omission selects Iceberg's current default. */
  public static final String SPEC_ID = "spec_id";
  /**
   * Iceberg / Gravitino table property for the current default partition spec id ({@code
   * default-spec-id}).
   */
  public static final String DEFAULT_SPEC_ID_PROPERTY = "default-spec-id";
  /** Rule key for caching option, or null for the Iceberg default. */
  public static final String USE_CACHING = "use_caching";

  /**
   * Default in-job / enqueue trigger: manifest count at or above Iceberg's {@code
   * commit.manifest.min-count-to-merge} default.
   */
  private static final String TRIGGER_EXPR =
      MANIFEST_NUMBER_METRIC + " >= " + MANIFEST_COUNT_THRESHOLD;

  private final Long manifestCountThreshold;
  private final Integer specId;
  private final Boolean useCaching;

  /** Creates default thresholds targeting Iceberg's current default partition spec. */
  public IcebergRewriteManifestsContent() {
    this(null, null, null, PolicyExecutionSpec.empty());
  }

  IcebergRewriteManifestsContent(
      @Nullable Long manifestCountThreshold,
      @Nullable Integer specId,
      @Nullable Boolean useCaching) {
    this(manifestCountThreshold, specId, useCaching, PolicyExecutionSpec.empty());
  }

  IcebergRewriteManifestsContent(
      @Nullable Long manifestCountThreshold,
      @Nullable Integer specId,
      @Nullable Boolean useCaching,
      @Nullable PolicyExecutionSpec executionSpec) {
    super(executionSpec);
    this.manifestCountThreshold =
        manifestCountThreshold == null ? DEFAULT_MANIFEST_COUNT_THRESHOLD : manifestCountThreshold;
    this.specId = specId;
    this.useCaching = useCaching;
  }

  /**
   * Returns the inclusive manifest-count trigger threshold.
   *
   * @return inclusive manifest-count threshold
   */
  public Long manifestCountThreshold() {
    return manifestCountThreshold;
  }

  /**
   * Returns the requested partition spec ID. When absent, the recommender resolves Iceberg's
   * current default from table metadata and retains it through job submission.
   *
   * @return requested partition spec ID, or null to use Iceberg's current default
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
        STRATEGY_TYPE_KEY, STRATEGY_TYPE_VALUE, JOB_TEMPLATE_NAME_KEY, JOB_TEMPLATE_NAME_VALUE);
  }

  @Override
  public Map<String, Object> rules() {
    Map<String, Object> rules = new LinkedHashMap<>();
    rules.put(MANIFEST_COUNT_THRESHOLD, manifestCountThreshold);
    rules.put(TRIGGER_EXPR_KEY, TRIGGER_EXPR);
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
    super.validate();
    Preconditions.checkArgument(manifestCountThreshold > 0, "manifest_count_threshold must be > 0");
    Preconditions.checkArgument(specId == null || specId >= 0, "spec_id must be >= 0");
  }

  /**
   * Parses {@link #DEFAULT_SPEC_ID_PROPERTY} from table properties.
   *
   * @param properties table properties; may be {@code null}
   * @return non-negative spec id, or {@code null} when missing / unparseable
   */
  @Nullable
  public static Integer parseDefaultSpecId(@Nullable Map<String, String> properties) {
    if (properties == null) {
      return null;
    }
    String value = properties.get(DEFAULT_SPEC_ID_PROPERTY);
    if (value == null || !value.matches("[0-9]+")) {
      return null;
    }
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Prefers an explicit policy {@code spec_id}; otherwise uses {@link #parseDefaultSpecId}.
   *
   * @param requestedSpecId explicit spec from policy content; may be {@code null}
   * @param properties table properties used when {@code requestedSpecId} is null
   * @return resolved spec id, or {@code null} when neither source yields one
   */
  @Nullable
  public static Integer resolveSpecId(
      @Nullable Integer requestedSpecId, @Nullable Map<String, String> properties) {
    return requestedSpecId != null ? requestedSpecId : parseDefaultSpecId(properties);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IcebergRewriteManifestsContent)) {
      return false;
    }
    IcebergRewriteManifestsContent that = (IcebergRewriteManifestsContent) other;
    return executionFieldsEqual(that)
        && Objects.equals(manifestCountThreshold, that.manifestCountThreshold)
        && Objects.equals(specId, that.specId)
        && Objects.equals(useCaching, that.useCaching);
  }

  @Override
  public int hashCode() {
    return Objects.hash(executionFieldsHashCode(), manifestCountThreshold, specId, useCaching);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("IcebergRewriteManifestsContent{");
    sb.append("manifestCountThreshold=")
        .append(manifestCountThreshold)
        .append(", specId=")
        .append(specId)
        .append(", useCaching=")
        .append(useCaching)
        .append(", ");
    appendExecutionFields(sb);
    sb.append('}');
    return sb.toString();
  }
}
