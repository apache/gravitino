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
package org.apache.gravitino.maintenance.optimizer.recommender.job;

import com.google.common.base.Preconditions;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.OrphanFileLocationUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.orphan.OrphanFileRemovalJobContext;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;

/** Converts orphan cleanup options to the existing Spark template's configuration. */
public class GravitinoOrphanFileRemovalJobAdapter implements GravitinoJobAdapter {
  private static final DateTimeFormatter TIMESTAMP =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ssXXX").withZone(ZoneOffset.UTC);
  private final Clock clock;

  /** Creates an adapter using the UTC system clock. */
  public GravitinoOrphanFileRemovalJobAdapter() {
    this(Clock.systemUTC());
  }

  GravitinoOrphanFileRemovalJobAdapter(Clock clock) {
    this.clock = clock;
  }

  @Override
  public Map<String, String> jobConfig(JobExecutionContext context) {
    Preconditions.checkArgument(
        context instanceof OrphanFileRemovalJobContext,
        "jobExecutionContext must be OrphanFileRemovalJobContext");
    OrphanFileRemovalJobContext orphan = (OrphanFileRemovalJobContext) context;
    Map<String, String> options = orphan.jobOptions();
    long days =
        Long.parseLong(
            options.getOrDefault(
                "olderThanDays",
                String.valueOf(IcebergOrphanFileRemovalContent.DEFAULT_OLDER_THAN_DAYS)));
    Preconditions.checkArgument(days >= 1, "olderThanDays must be at least 1");
    String dryRun = options.getOrDefault("dryRun", "false");
    Preconditions.checkArgument(
        "true".equals(dryRun) || "false".equals(dryRun), "dryRun must be true or false");
    String location = options.getOrDefault("location", "");
    if (options.containsKey("location")) {
      Preconditions.checkArgument(
          orphan.tableLocation() != null,
          "Table location is required to validate a custom scan location");
      OrphanFileLocationUtils.validateLocation(orphan.tableLocation(), location);
      location = OrphanFileLocationUtils.normalizeLocation(location).toString();
    }
    return Map.of(
        "table_identifier",
        IdentifierUtils.removeCatalogFromIdentifier(orphan.nameIdentifier()).toString(),
        "older_than",
        TIMESTAMP.format(clock.instant().minus(Duration.ofDays(days))),
        "location",
        location,
        "dry_run",
        dryRun);
  }
}
