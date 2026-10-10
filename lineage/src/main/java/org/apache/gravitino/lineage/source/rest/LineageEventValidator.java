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

package org.apache.gravitino.lineage.source.rest;

import com.google.common.base.Preconditions;
import io.openlineage.server.OpenLineage.Dataset;
import io.openlineage.server.OpenLineage.RunEvent;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** Validates the required fields of an OpenLineage run event. */
public final class LineageEventValidator {

  private LineageEventValidator() {}

  /**
   * Validates the required fields of an OpenLineage run event.
   *
   * @param event event to validate
   * @throws IllegalArgumentException if a required field is absent or blank
   */
  public static void validate(RunEvent event) {
    Preconditions.checkArgument(event != null, "Lineage event cannot be null");
    Preconditions.checkArgument(event.getEventTime() != null, "eventTime is required");
    Preconditions.checkArgument(event.getProducer() != null, "producer is required");
    Preconditions.checkArgument(event.getSchemaURL() != null, "schemaURL is required");
    Preconditions.checkArgument(event.getRun() != null, "run is required");
    Preconditions.checkArgument(event.getRun().getRunId() != null, "run.runId is required");
    Preconditions.checkArgument(event.getJob() != null, "job is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(event.getJob().getNamespace()), "job.namespace is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(event.getJob().getName()), "job.name is required");

    validateDatasets(event.getInputs(), "inputs");
    validateDatasets(event.getOutputs(), "outputs");
  }

  private static void validateDatasets(List<? extends Dataset> datasets, String fieldName) {
    if (datasets == null) {
      return;
    }

    for (int index = 0; index < datasets.size(); index++) {
      Dataset dataset = datasets.get(index);
      Preconditions.checkArgument(dataset != null, "%s[%s] cannot be null", fieldName, index);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(dataset.getNamespace()),
          "%s[%s].namespace is required",
          fieldName,
          index);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(dataset.getName()), "%s[%s].name is required", fieldName, index);
    }
  }
}
