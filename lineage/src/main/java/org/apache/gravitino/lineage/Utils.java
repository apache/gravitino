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

import io.openlineage.client.OpenLineage;
import io.openlineage.server.OpenLineage.Job;
import io.openlineage.server.OpenLineage.Run;
import io.openlineage.server.OpenLineage.RunEvent;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {
  private Utils() {}

  public static String getRunID(RunEvent event) {
    Run run = event.getRun();
    return run == null ? "Unknown" : run.getRunId().toString();
  }

  public static String getJobName(RunEvent event) {
    Job job = event.getJob();
    return job == null ? "Unknown" : job.getName();
  }

  public static OpenLineage.RunEvent mapLineageServertoClientRunEvent(
      io.openlineage.server.OpenLineage.RunEvent serverEvent) {
    OpenLineage ol = new OpenLineage(serverEvent.getSchemaURL());

    return ol.newRunEventBuilder()
        .eventType(OpenLineage.RunEvent.EventType.valueOf(serverEvent.getEventType().name()))
        .eventTime(serverEvent.getEventTime())
        .run(toClientRun(serverEvent.getRun(), ol))
        .job(toClientJob(serverEvent.getJob(), ol))
        .inputs(toClientInputs(serverEvent.getInputs(), ol))
        .outputs(toClientOutputs(serverEvent.getOutputs(), ol))
        .build();
  }

  private static OpenLineage.Run toClientRun(
      io.openlineage.server.OpenLineage.Run serverRun, OpenLineage ol) {
    return ol.newRun(serverRun.getRunId(), ol.newRunFacetsBuilder().build());
  }

  private static OpenLineage.Job toClientJob(
      io.openlineage.server.OpenLineage.Job serverJob, OpenLineage ol) {
    return ol.newJobBuilder()
        .namespace(serverJob.getNamespace())
        .name(serverJob.getName())
        .facets(ol.newJobFacetsBuilder().build())
        .build();
  }

  private static List<OpenLineage.InputDataset> toClientInputs(
      List<io.openlineage.server.OpenLineage.InputDataset> serverInputs, OpenLineage ol) {
    return serverInputs.stream()
        .map(
            input ->
                ol.newInputDatasetBuilder()
                    .namespace(input.getNamespace())
                    .name(input.getName())
                    .facets(ol.newDatasetFacetsBuilder().build())
                    .build())
        .collect(Collectors.toList());
  }

  private static List<OpenLineage.OutputDataset> toClientOutputs(
      List<io.openlineage.server.OpenLineage.OutputDataset> serverOutputs, OpenLineage ol) {
    return serverOutputs.stream()
        .map(
            output ->
                ol.newOutputDatasetBuilder()
                    .namespace(output.getNamespace())
                    .name(output.getName())
                    .facets(ol.newDatasetFacetsBuilder().build())
                    .build())
        .collect(Collectors.toList());
  }
}
