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
import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.snapshot.SnapshotExpirationJobContext;
import org.apache.gravitino.policy.IcebergSnapshotExpirationContent;

/** Converts snapshot-expiration options to the built-in Spark template configuration. */
public class GravitinoSnapshotExpirationJobAdapter implements GravitinoJobAdapter {
  private static final String TABLE_IDENTIFIER_KEY = "table_identifier";
  private static final String OLDER_THAN_KEY = "older_than";
  private static final String RETAIN_LAST_KEY = "retain_last";
  private static final String STREAM_RESULTS_KEY = "stream_results";
  private static final DateTimeFormatter TIMESTAMP =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ssXXX").withZone(ZoneOffset.UTC);
  private final Clock clock;

  /** Creates an adapter using the UTC system clock. */
  public GravitinoSnapshotExpirationJobAdapter() {
    this(Clock.systemUTC());
  }

  GravitinoSnapshotExpirationJobAdapter(Clock clock) {
    this.clock = clock;
  }

  @Override
  public Map<String, String> jobConfig(JobExecutionContext context) {
    Preconditions.checkArgument(
        context instanceof SnapshotExpirationJobContext,
        "jobExecutionContext must be SnapshotExpirationJobContext");
    SnapshotExpirationJobContext expire = (SnapshotExpirationJobContext) context;
    Map<String, String> options = expire.jobOptions();
    long days;
    try {
      days =
          Long.parseLong(
              options.getOrDefault(
                  IcebergSnapshotExpirationContent.OLDER_THAN_DAYS_KEY,
                  String.valueOf(IcebergSnapshotExpirationContent.DEFAULT_OLDER_THAN_DAYS)));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("olderThanDays must be an integer", e);
    }
    IcebergSnapshotExpirationContent.validateOlderThanDays(days);

    long retainLast;
    try {
      retainLast =
          Long.parseLong(
              options.getOrDefault(
                  IcebergSnapshotExpirationContent.RETAIN_LAST_KEY,
                  String.valueOf(IcebergSnapshotExpirationContent.DEFAULT_RETAIN_LAST)));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("retainLast must be an integer", e);
    }
    IcebergSnapshotExpirationContent.validateRetainLast(retainLast);

    String streamResults =
        options.getOrDefault(
            IcebergSnapshotExpirationContent.STREAM_RESULTS_KEY,
            String.valueOf(IcebergSnapshotExpirationContent.DEFAULT_STREAM_RESULTS));
    Preconditions.checkArgument(
        "true".equalsIgnoreCase(streamResults) || "false".equalsIgnoreCase(streamResults),
        "streamResults must be true or false");

    Map<String, String> config = new LinkedHashMap<>();
    config.put(
        TABLE_IDENTIFIER_KEY,
        IdentifierUtils.removeCatalogFromIdentifier(expire.nameIdentifier()).toString());
    config.put(OLDER_THAN_KEY, cutoff(days));
    config.put(RETAIN_LAST_KEY, Long.toString(retainLast));
    config.put(STREAM_RESULTS_KEY, Boolean.toString(Boolean.parseBoolean(streamResults)));
    return Map.copyOf(config);
  }

  private String cutoff(long days) {
    try {
      return TIMESTAMP.format(clock.instant().minus(days, ChronoUnit.DAYS));
    } catch (DateTimeException | ArithmeticException e) {
      throw new IllegalArgumentException(
          "olderThanDays cannot be represented as an expiration timestamp", e);
    }
  }
}
