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

package org.apache.gravitino.maintenance.optimizer.monitor.job.local;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.monitor.JobProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/**
 * A {@link JobProvider} that reads table-to-jobs mappings from a local JSON-lines file.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>Set {@link OptimizerConfig#JOB_PROVIDER_CONFIG} to {@value #NAME}.
 *   <li>Set {@link #JOB_FILE_PATH_CONFIG} ({@code
 *       gravitino.optimizer.monitor.localJobProvider.filePath}) to a readable JSON-lines file path.
 *   <li>Optional: set {@link OptimizerConfig#GRAVITINO_DEFAULT_CATALOG_CONFIG} to resolve two-level
 *       table identifiers ({@code schema.table}) into {@code <defaultCatalog>.schema.table}.
 * </ul>
 *
 * <p>Each line in the job file is a JSON object:
 *
 * <pre>{@code
 * {"identifier":"catalog.schema.table","job-identifiers":["job1","job2"]}
 * {"identifier":"schema.table","job-identifiers":["job3"]}
 * }</pre>
 *
 * <p>Format rules:
 *
 * <ul>
 *   <li>{@code identifier}: table name, either {@code catalog.schema.table} or {@code schema.table}
 *       (when default catalog is configured).
 *   <li>{@code job-identifiers}: array of job identifiers, each parseable by {@link
 *       NameIdentifier#parse(String)}.
 *   <li>Malformed lines and invalid entries are skipped with warning logs.
 * </ul>
 */
public class LocalJobProvider implements JobProvider {

  public static final String NAME = "local-job-provider";
  public static final String CONFIG_NAME = "localJobProvider";

  /** Config key for the local JSON-lines job mapping file path. */
  public static final String JOB_FILE_PATH_CONFIG =
      OptimizerConfig.MONITOR_PREFIX + CONFIG_NAME + ".filePath";

  private FileJobReader jobReader;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    String path = optimizerEnv.config().getRawString(JOB_FILE_PATH_CONFIG);
    if (StringUtils.isBlank(path)) {
      throw new IllegalArgumentException(
          JOB_FILE_PATH_CONFIG + " must be provided for LocalJobProvider");
    }
    Path jobFilePath = Path.of(path).toAbsolutePath().normalize();
    if (!Files.exists(jobFilePath)
        || !Files.isRegularFile(jobFilePath)
        || !Files.isReadable(jobFilePath)) {
      throw new IllegalArgumentException(
          "Configured job file path is not a readable file: " + jobFilePath);
    }
    String defaultCatalog =
        optimizerEnv.config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG);
    this.jobReader = new FileJobReader(jobFilePath, defaultCatalog);
  }

  @Override
  public List<NameIdentifier> jobIdentifiers(NameIdentifier tableIdentifier) {
    if (jobReader == null) {
      throw new IllegalStateException("LocalJobProvider is not initialized");
    }
    return jobReader.readJobIdentifiers(tableIdentifier);
  }

  @Override
  public void close() throws Exception {}
}
