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

/** Configuration for table-level Iceberg orphan cleanup on an optimizer invocation. */
public class IcebergOrphanFileRemovalContent implements PolicyContent {
  /** Strategy type for orphan cleanup. */
  public static final String STRATEGY_TYPE_VALUE = "iceberg-orphan-file-removal";
  /** Existing Spark job template used for cleanup. */
  public static final String JOB_TEMPLATE_NAME_VALUE = "builtin-iceberg-remove-orphan-files";
  /** Default retention in days. */
  public static final long DEFAULT_OLDER_THAN_DAYS = 3;
  /** Default execution mode. */
  public static final boolean DEFAULT_DRY_RUN = false;

  private final long olderThanDays;
  @Nullable private final String location;
  private final boolean dryRun;

  private IcebergOrphanFileRemovalContent() {
    this(DEFAULT_OLDER_THAN_DAYS, null, DEFAULT_DRY_RUN);
  }

  IcebergOrphanFileRemovalContent(long olderThanDays, @Nullable String location, boolean dryRun) {
    this.olderThanDays = olderThanDays;
    this.location = location;
    this.dryRun = dryRun;
  }

  /**
   * @return minimum age of files eligible for removal, in days
   */
  public long olderThanDays() {
    return olderThanDays;
  }

  /**
   * @return optional scan location within the table, or null for the table location
   */
  @Nullable
  public String location() {
    return location;
  }

  /**
   * @return whether candidates should only be listed
   */
  public boolean dryRun() {
    return dryRun;
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
    // Eligibility is evaluated when the optimizer is invoked. Scheduling is owned by the caller.
    rules.put("trigger-expr", "true");
    rules.put("score-expr", "1");
    rules.put("job.options.olderThanDays", olderThanDays);
    rules.put("job.options.dryRun", dryRun);
    if (location != null) {
      rules.put("job.options.location", location);
    }
    return Collections.unmodifiableMap(rules);
  }

  @Override
  public void validate() {
    PolicyContent.super.validate();
    Preconditions.checkArgument(olderThanDays >= 1, "olderThanDays must be at least 1");
    Preconditions.checkArgument(
        location == null || !location.trim().isEmpty(), "location must not be blank");
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IcebergOrphanFileRemovalContent)) {
      return false;
    }
    IcebergOrphanFileRemovalContent that = (IcebergOrphanFileRemovalContent) other;
    return olderThanDays == that.olderThanDays
        && dryRun == that.dryRun
        && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(olderThanDays, location, dryRun);
  }
}
