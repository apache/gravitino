/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lineage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.server.OpenLineage.InputDataset;
import io.openlineage.server.OpenLineage.Job;
import io.openlineage.server.OpenLineage.OutputDataset;
import io.openlineage.server.OpenLineage.Run;
import io.openlineage.server.OpenLineage.RunEvent;
import io.openlineage.server.OpenLineage.RunEvent.EventType;
import io.openlineage.server.OpenLineage.RunFacets;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRunEventUtils {

  @SneakyThrows
  @Test
  void testRunEventUtils() {
    List<File> files = getRunEventJsonFiles();
    files.forEach(
        file -> {
          try {
            testRunEventJsonFile(file);
          } catch (Exception e) {
            Assertions.fail("Failed to test file: " + file.getName(), e);
          }
        });
  }

  @SneakyThrows
  private void testRunEventJsonFile(File jsonFile) {
    ObjectMapper objectMapper = ObjectMapperProvider.objectMapper();
    RunEvent serverEvent = objectMapper.readValue(jsonFile, RunEvent.class);
    OpenLineage.RunEvent clientEvent = RunEventUtils.getClientRunEvent(serverEvent);
    checkRunEvent(serverEvent, clientEvent);
  }

  private List<File> getRunEventJsonFiles() {
    String projectDir = System.getenv("GRAVITINO_ROOT_DIR");

    String path =
        Paths.get(projectDir, "server", "src", "test", "resources", "lineage")
            .toAbsolutePath()
            .toString();
    List<File> jsonFiles = new ArrayList<>();
    File directory = new File(path);

    if (directory.exists() && directory.isDirectory()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isFile() && file.getName().endsWith(".json")) {
            jsonFiles.add(file);
          }
        }
      }
    }
    return jsonFiles;
  }

  private void checkRunEvent(RunEvent serverEvent, OpenLineage.RunEvent clientEvent) {
    // basic check
    Assertions.assertEquals(serverEvent.getEventTime(), clientEvent.getEventTime());
    Assertions.assertEquals(
        getClientEventType(serverEvent.getEventType()), clientEvent.getEventType());
    Assertions.assertEquals(serverEvent.getProducer(), clientEvent.getProducer());
    Assertions.assertEquals(serverEvent.getSchemaURL(), clientEvent.getSchemaURL());

    // check job
    checkJob(serverEvent.getJob(), clientEvent.getJob());

    // check run
    checkRun(serverEvent.getRun(), clientEvent.getRun());

    // check input
    checkInputs(serverEvent.getInputs(), clientEvent.getInputs());

    // check output
    checkOutputs(serverEvent.getOutputs(), clientEvent.getOutputs());
  }

  private void checkInputs(
      List<InputDataset> serverInputs, List<OpenLineage.InputDataset> clientInputs) {
    Assertions.assertEquals(serverInputs.size(), clientInputs.size());
    HashMap<String, Set<String>> serverInputMap = new HashMap<>();
    serverInputs.stream()
        .forEach(
            serverInput -> {
              Set<String> facets = new HashSet<>();
              if (serverInput.getFacets() != null) {
                facets.addAll(serverInput.getFacets().getAdditionalProperties().keySet());
              }
              if (serverInput.getInputFacets() != null) {
                facets.addAll(serverInput.getInputFacets().getAdditionalProperties().keySet());
              }
              serverInputMap.put(serverInput.getName() + serverInput.getNamespace(), facets);
            });

    clientInputs.stream()
        .forEach(
            clientInput -> {
              Set<String> serverFacets =
                  serverInputMap.get(clientInput.getName() + clientInput.getNamespace());
              Assertions.assertNotNull(serverFacets);
              Set<String> clientFacets =
                  getClientInputFacets(clientInput.getFacets(), clientInput.getInputFacets());
              Assertions.assertEquals(serverFacets, clientFacets);
            });
  }

  private void checkOutputs(
      List<OutputDataset> serverOutputs, List<OpenLineage.OutputDataset> clientOutputs) {
    Assertions.assertEquals(serverOutputs.size(), clientOutputs.size());
    HashMap<String, Set<String>> serverInputMap = new HashMap<>();
    serverOutputs.stream()
        .forEach(
            serverOutput -> {
              Set<String> facets = new HashSet<>();
              if (serverOutput.getFacets() != null) {
                facets.addAll(serverOutput.getFacets().getAdditionalProperties().keySet());
              }
              if (serverOutput.getOutputFacets() != null) {
                facets.addAll(serverOutput.getOutputFacets().getAdditionalProperties().keySet());
              }
              serverInputMap.put(serverOutput.getName() + serverOutput.getNamespace(), facets);
            });

    clientOutputs.stream()
        .forEach(
            clientOutput -> {
              Set<String> serverFacets =
                  serverInputMap.get(clientOutput.getName() + clientOutput.getNamespace());
              Assertions.assertNotNull(serverFacets);
              Set<String> clientFacets =
                  getClientOutputFacets(clientOutput.getFacets(), clientOutput.getOutputFacets());
              Assertions.assertEquals(serverFacets, clientFacets);
            });
  }

  private Set<String> getClientOutputFacets(
      DatasetFacets clientOutputFacets,
      OpenLineage.OutputDatasetOutputFacets clientOutputOutputFacets) {
    HashSet<String> sets = new HashSet<>();
    sets.addAll(getDatasetFacets(clientOutputFacets));

    if (clientOutputOutputFacets == null) {
      return sets;
    }
    sets.addAll(clientOutputOutputFacets.getAdditionalProperties().keySet());

    if (clientOutputOutputFacets.getOutputStatistics() != null) {
      sets.add("outputStatistics");
    }
    if (clientOutputOutputFacets.getIceberg_scan_report() != null) {
      sets.add("iceberg_scan_report");
    }

    return sets;
  }

  private Set<String> getDatasetFacets(DatasetFacets clientFacets) {
    HashSet<String> sets = new HashSet<>();
    if (clientFacets == null) {
      return sets;
    }
    sets.addAll(clientFacets.getAdditionalProperties().keySet());

    if (clientFacets.getDocumentation() != null) {
      sets.add("documentation");
    }
    if (clientFacets.getOwnership() != null) {
      sets.add("ownership");
    }
    if (clientFacets.getSchema() != null) {
      sets.add("schema");
    }
    if (clientFacets.getTags() != null) {
      sets.add("tags");
    }
    if (clientFacets.getColumnLineage() != null) {
      sets.add("columnLineage");
    }
    if (clientFacets.getDatasetType() != null) {
      sets.add("datasetType");
    }
    if (clientFacets.getDataSource() != null) {
      sets.add("dataSource");
    }
    if (clientFacets.getStorage() != null) {
      sets.add("storage");
    }
    if (clientFacets.getLifecycleStateChange() != null) {
      sets.add("lifecycleStateChange");
    }
    if (clientFacets.getSymlinks() != null) {
      sets.add("symlinks");
    }

    return sets;
  }

  private Set<String> getClientInputFacets(
      DatasetFacets clientInputFacets, OpenLineage.InputDatasetInputFacets clientInputInputFacets) {
    HashSet<String> sets = new HashSet<>();
    sets.addAll(getDatasetFacets(clientInputFacets));

    if (clientInputInputFacets == null) {
      return sets;
    }
    sets.addAll(clientInputInputFacets.getAdditionalProperties().keySet());
    if (clientInputInputFacets.getInputStatistics() != null) {
      sets.add("inputStatistics");
    }
    if (clientInputInputFacets.getDataQualityAssertions() != null) {
      sets.add("dataQualityAssertions");
    }
    if (clientInputInputFacets.getDataQualityMetrics() != null) {
      sets.add("dataQualityMetrics");
    }
    if (clientInputInputFacets.getIceberg_scan_report() != null) {
      sets.add("iceberg_scan_report");
    }
    return sets;
  }

  private void checkJob(Job serverJob, OpenLineage.Job clientJob) {
    // basic check
    Assertions.assertEquals(serverJob.getName(), clientJob.getName());
    Assertions.assertEquals(serverJob.getNamespace(), clientJob.getNamespace());
    // Assertions.assertEquals(serverJob.getFacets(), clientJob.getFacets());
    Set<String> serverJobFacets = serverJob.getFacets().getAdditionalProperties().keySet();
    Set<String> clientJobFacets = getClientJobFacets(clientJob.getFacets());
    Assertions.assertEquals(serverJobFacets, clientJobFacets);
  }

  private Set<String> getClientJobFacets(OpenLineage.JobFacets clientJobFacets) {
    HashSet<String> sets = new HashSet<>();
    sets.addAll(clientJobFacets.getAdditionalProperties().keySet());
    if (clientJobFacets.getJobType() != null) {
      sets.add("jobType");
    }
    if (clientJobFacets.getDocumentation() != null) {
      sets.add("documentation");
    }
    if (clientJobFacets.getOwnership() != null) {
      sets.add("ownership");
    }
    if (clientJobFacets.getGcp_lineage() != null) {
      sets.add("gcp_lineage");
    }
    if (clientJobFacets.getSql() != null) {
      sets.add("sql");
    }
    if (clientJobFacets.getSourceCode() != null) {
      sets.add("sourceCode");
    }
    if (clientJobFacets.getSourceCodeLocation() != null) {
      sets.add("sourceCodeLocation");
    }
    return sets;
  }

  private void checkRun(Run serverRun, OpenLineage.Run clientRun) {
    // basic check
    Assertions.assertEquals(serverRun.getRunId(), clientRun.getRunId());

    Set<String> serverRunFacets = getServerRunFacets(serverRun.getFacets());
    Set<String> clientRunFacets = getClientRunFacets(clientRun.getFacets());
    Assertions.assertEquals(serverRunFacets, clientRunFacets);
  }

  private Set<String> getServerRunFacets(RunFacets serverRunFacets) {
    if (serverRunFacets == null) {
      return new HashSet<>();
    }
    return serverRunFacets.getAdditionalProperties().keySet();
  }

  private Set<String> getClientRunFacets(OpenLineage.RunFacets clientRunFacets) {
    HashSet<String> sets = new HashSet<>();
    if (clientRunFacets == null) {
      return sets;
    }
    sets.addAll(clientRunFacets.getAdditionalProperties().keySet());
    if (clientRunFacets.getErrorMessage() != null) {
      sets.add("errorMessage");
    }
    if (clientRunFacets.getEnvironmentVariables() != null) {
      sets.add("environmentVariables");
    }
    if (clientRunFacets.getParent() != null) {
      sets.add("parent");
    }
    if (clientRunFacets.getExternalQuery() != null) {
      sets.add("externalQuery");
    }

    if (clientRunFacets.getExtractionError() != null) {
      sets.add("extractionError");
    }
    if (clientRunFacets.getGcp_dataproc() != null) {
      sets.add("gcp_dataproc");
    }
    if (clientRunFacets.getNominalTime() != null) {
      sets.add("nominalTime");
    }
    if (clientRunFacets.getTags() != null) {
      sets.add("tags");
    }
    if (clientRunFacets.getNominalTime() != null) {
      sets.add("nominalTime");
    }
    if (clientRunFacets.getProcessing_engine() != null) {
      sets.add("processing_engine");
    }
    return sets;
  }

  private OpenLineage.RunEvent.EventType getClientEventType(EventType serverType) {
    switch (serverType) {
      case START:
        return OpenLineage.RunEvent.EventType.START;
      case RUNNING:
        return OpenLineage.RunEvent.EventType.RUNNING;
      case COMPLETE:
        return OpenLineage.RunEvent.EventType.COMPLETE;
      case ABORT:
        return OpenLineage.RunEvent.EventType.ABORT;
      case FAIL:
        return OpenLineage.RunEvent.EventType.FAIL;
      case OTHER:
        return OpenLineage.RunEvent.EventType.OTHER;
      default:
    }
    throw new RuntimeException("Should not come here.");
  }
}
