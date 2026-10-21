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
package org.apache.gravitino.maintenance.jobs;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergExpireSnapshotsJob;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergRemoveOrphanFilesJob;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergRewriteDataFilesJob;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergRewriteManifestsJob;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergUpdateManifestStatsJob;
import org.apache.gravitino.maintenance.jobs.iceberg.IcebergUpdateStatsAndMetricsJob;
import org.apache.gravitino.maintenance.jobs.iceberg.TmsIcebergMaintenanceJob;
import org.apache.gravitino.maintenance.jobs.spark.SparkPiJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides built-in job templates bundled with the Gravitino jobs module. */
public class BuiltInJobTemplateProvider implements JobTemplateProvider {

  private static final Logger LOG = LoggerFactory.getLogger(BuiltInJobTemplateProvider.class);

  private static final Pattern NAME_PATTERN =
      Pattern.compile(JobTemplateProvider.BUILTIN_NAME_PATTERN);
  private static final Pattern VERSION_PATTERN =
      Pattern.compile(JobTemplateProvider.VERSION_VALUE_PATTERN);

  /**
   * Lazy suppliers so a linkage failure in one Spark job class does not prevent the provider SPI
   * class from loading (or other templates from registering).
   */
  // Lambdas (not method refs) so job classes are not loaded until jobTemplates() runs.
  private static final List<Supplier<BuiltInJob>> BUILT_IN_JOB_SUPPLIERS =
      ImmutableList.of(
          () -> new SparkPiJob(),
          () -> new IcebergRewriteDataFilesJob(),
          () -> new IcebergRewriteManifestsJob(),
          () -> new IcebergUpdateStatsAndMetricsJob(),
          () -> new IcebergUpdateManifestStatsJob(),
          () -> new IcebergExpireSnapshotsJob(),
          () -> new IcebergRemoveOrphanFilesJob(),
          () -> new TmsIcebergMaintenanceJob());

  @Override
  public List<? extends JobTemplate> jobTemplates() {
    List<JobTemplate> templates = new ArrayList<>();
    for (Supplier<BuiltInJob> supplier : BUILT_IN_JOB_SUPPLIERS) {
      try {
        JobTemplate template = supplier.get().jobTemplate();
        if (isValid(template)) {
          templates.add(template);
        }
      } catch (LinkageError | RuntimeException e) {
        LOG.warn("Skip built-in job template that failed to load: {}", e.toString());
      }
    }
    return templates;
  }

  private boolean isValid(JobTemplate template) {
    if (!NAME_PATTERN.matcher(template.name()).matches()) {
      LOG.warn("Skip built-in job template with illegal name: {}", template.name());
      return false;
    }

    Optional<String> version =
        Optional.ofNullable(template.customFields())
            .map(fields -> fields.get(JobTemplateProvider.PROPERTY_VERSION_KEY));
    if (!version.isPresent() || !VERSION_PATTERN.matcher(version.get()).matches()) {
      LOG.warn("Skip built-in job template {} without valid version", template.name());
      return false;
    }

    return true;
  }
}
