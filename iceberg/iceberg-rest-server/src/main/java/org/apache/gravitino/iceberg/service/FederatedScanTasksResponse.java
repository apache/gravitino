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

package org.apache.gravitino.iceberg.service;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.responses.FetchScanTasksResponseParser;

/**
 * Adapts remote scan tasks without re-evaluating their residual filters.
 *
 * <p>A fetch request carries an opaque plan task, so the original scan's case sensitivity is not
 * available here. Iceberg's standard parser creates residual evaluators that bind column names when
 * the response is serialized. The remote has already evaluated these residuals against each
 * partition; preserve them as supplied instead of binding them again with an assumed setting.
 */
final class FederatedScanTasksResponse implements RESTResponse {

  private final JsonNode json;

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  FederatedScanTasksResponse(JsonNode json) {
    this.json = json;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(json != null && json.isObject(), "Invalid scan tasks response");
  }

  @SuppressWarnings("deprecation")
  FetchScanTasksResponse toResponse(Map<Integer, PartitionSpec> specsById) {
    // Use Iceberg's parser for file metadata, partition values and delete-file references. Its
    // evaluators are lazy, and are replaced below before any residual is evaluated.
    FetchScanTasksResponse parsed = FetchScanTasksResponseParser.fromJson(json, specsById, true);
    if (parsed.fileScanTasks() == null || parsed.fileScanTasks().isEmpty()) {
      return parsed;
    }

    List<FileScanTask> tasks = new ArrayList<>();
    JsonNode taskNodes = json.get("file-scan-tasks");
    for (int i = 0; i < parsed.fileScanTasks().size(); i++) {
      FileScanTask task = parsed.fileScanTasks().get(i);
      JsonNode taskNode = taskNodes.get(i);
      Expression residual =
          taskNode.has("residual-filter")
              ? ExpressionParser.fromJson(taskNode.get("residual-filter"))
              : null;
      tasks.add(
          new BaseFileScanTask(
              task.file(),
              task.deletes().toArray(new DeleteFile[0]),
              SchemaParser.toJson(task.schema()),
              PartitionSpecParser.toJson(task.spec()),
              ResidualEvaluator.unpartitioned(residual)));
    }
    return FetchScanTasksResponse.builder()
        .withFileScanTasks(tasks)
        .withPlanTasks(parsed.planTasks())
        .withSpecsById(parsed.specsById())
        .build();
  }
}
