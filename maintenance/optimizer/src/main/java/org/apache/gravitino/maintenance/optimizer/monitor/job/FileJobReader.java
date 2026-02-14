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

package org.apache.gravitino.maintenance.optimizer.monitor.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared reader for file-based job providers. */
class FileJobReader {

  private static final Logger LOG = LoggerFactory.getLogger(FileJobReader.class);
  private static final String IDENTIFIER_FIELD = "identifier";
  private static final String JOB_IDENTIFIERS_FIELD = "job-identifiers";

  private final Path jobFilePath;
  private final String defaultCatalogName;

  FileJobReader(Path jobFilePath, String defaultCatalogName) {
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

        JsonNode node = parseJson(line, lineNumber);
        if (node == null) {
          continue;
        }

        NameIdentifier identifier = parseIdentifier(node.get(IDENTIFIER_FIELD), lineNumber);
        if (identifier == null || !identifier.equals(tableIdentifier)) {
          continue;
        }

        collectJobs(node.get(JOB_IDENTIFIERS_FIELD), jobs, lineNumber);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read job mappings", e);
    }

    return List.copyOf(jobs);
  }

  private JsonNode parseJson(String line, int lineNumber) {
    try {
      return JsonUtils.anyFieldMapper().readTree(line);
    } catch (IOException e) {
      LOG.warn("Skip malformed job mapping at line {}: {}", lineNumber, line);
      return null;
    }
  }

  private NameIdentifier parseIdentifier(JsonNode identifierNode, int lineNumber) {
    if (identifierNode == null || identifierNode.isNull() || !identifierNode.isTextual()) {
      return null;
    }
    String identifierText = identifierNode.asText();
    try {
      NameIdentifier parsed = NameIdentifier.parse(identifierText);
      int levels = parsed.namespace().levels().length;
      if (levels == 1 && StringUtils.isNotBlank(defaultCatalogName)) {
        return NameIdentifier.of(defaultCatalogName, parsed.namespace().levels()[0], parsed.name());
      } else if (levels == 2) {
        return parsed;
      }
      LOG.warn("Skip invalid table identifier at line {}: {}", lineNumber, identifierText);
      return null;
    } catch (Exception e) {
      LOG.warn("Skip invalid table identifier at line {}: {}", lineNumber, identifierText);
      return null;
    }
  }

  private void collectJobs(JsonNode jobsNode, Set<NameIdentifier> jobs, int lineNumber) {
    if (jobsNode == null || jobsNode.isNull() || !jobsNode.isArray()) {
      return;
    }
    Iterator<JsonNode> iterator = jobsNode.elements();
    while (iterator.hasNext()) {
      JsonNode jobNode = iterator.next();
      if (jobNode == null || jobNode.isNull() || !jobNode.isTextual()) {
        continue;
      }
      String jobText = jobNode.asText();
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
}
