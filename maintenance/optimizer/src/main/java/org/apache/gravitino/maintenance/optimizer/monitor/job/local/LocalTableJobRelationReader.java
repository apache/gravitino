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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared reader for file-based table-job relation providers. */
class LocalTableJobRelationReader {

  private static final Logger LOG = LoggerFactory.getLogger(LocalTableJobRelationReader.class);

  private final Path jobFilePath;
  private final String defaultCatalogName;

  LocalTableJobRelationReader(Path jobFilePath, String defaultCatalogName) {
    Preconditions.checkArgument(jobFilePath != null, "jobFilePath cannot be null");
    this.jobFilePath = jobFilePath;
    this.defaultCatalogName = defaultCatalogName;
  }

  List<NameIdentifier> readJobIdentifiers(NameIdentifier tableIdentifier) {
    Set<NameIdentifier> jobs = new LinkedHashSet<>();

    try (BufferedReader reader = Files.newBufferedReader(jobFilePath, StandardCharsets.UTF_8)) {
      String line;
      int lineNumber = 0;
      while ((line = reader.readLine()) != null) {
        lineNumber++;
        if (StringUtils.isBlank(line)) {
          continue;
        }

        JobMappingLine mappingLine = parseJsonLine(line, lineNumber);
        if (mappingLine == null) {
          continue;
        }

        Optional<NameIdentifier> identifier = parseIdentifier(mappingLine.identifier, lineNumber);
        if (identifier.isEmpty() || !identifier.get().equals(tableIdentifier)) {
          continue;
        }

        collectJobs(mappingLine.jobIdentifiers, jobs, lineNumber);
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to read job mappings from file %s for table %s",
              jobFilePath, tableIdentifier),
          e);
    }

    return List.copyOf(jobs);
  }

  private JobMappingLine parseJsonLine(String line, int lineNumber) {
    try {
      return JsonUtils.anyFieldMapper().readValue(line, JobMappingLine.class);
    } catch (IOException e) {
      LOG.warn("Skip malformed job mapping at line {}: {}", lineNumber, line);
      return null;
    }
  }

  private Optional<NameIdentifier> parseIdentifier(String identifierText, int lineNumber) {
    if (StringUtils.isBlank(identifierText)) {
      return Optional.empty();
    }

    Optional<NameIdentifier> parsed =
        IdentifierUtils.parseTableIdentifier(identifierText, defaultCatalogName);
    if (parsed.isEmpty()) {
      LOG.warn("Skip invalid table identifier at line {}: {}", lineNumber, identifierText);
    }
    return parsed;
  }

  private void collectJobs(List<Object> jobIdentifiers, Set<NameIdentifier> jobs, int lineNumber) {
    if (jobIdentifiers == null || jobIdentifiers.isEmpty()) {
      return;
    }
    for (Object jobIdentifier : jobIdentifiers) {
      if (!(jobIdentifier instanceof String)) {
        continue;
      }
      String jobText = (String) jobIdentifier;
      if (StringUtils.isBlank(jobText)) {
        continue;
      }
      try {
        jobs.add(NameIdentifier.parse(jobText));
      } catch (Exception e) {
        LOG.warn("Skip invalid job identifier at line {}: {}", lineNumber, jobText);
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class JobMappingLine {
    @JsonProperty("identifier")
    private String identifier;

    @JsonProperty("job-identifiers")
    private List<Object> jobIdentifiers;
  }
}
