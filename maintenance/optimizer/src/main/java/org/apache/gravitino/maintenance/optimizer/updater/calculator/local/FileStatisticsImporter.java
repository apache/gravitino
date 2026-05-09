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

package org.apache.gravitino.maintenance.optimizer.updater.calculator.local;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** Shared importer for file-based table statistics. */
public class FileStatisticsImporter extends AbstractStatisticsImporter {

  private final Path statisticsFilePath;

  public FileStatisticsImporter(Path statisticsFilePath, String defaultCatalogName) {
    super(defaultCatalogName);
    Preconditions.checkArgument(statisticsFilePath != null, "statisticsFilePath must not be null");
    this.statisticsFilePath = statisticsFilePath;
  }

  @Override
  protected BufferedReader openReader() throws IOException {
    return Files.newBufferedReader(statisticsFilePath, StandardCharsets.UTF_8);
  }
}
