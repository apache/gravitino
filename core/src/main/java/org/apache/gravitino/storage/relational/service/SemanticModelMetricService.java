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
package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.semantic.MetricDTO;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.semantic.ImmutableSemanticModelMetric;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricListingPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NamespaceUtil;

/** Reads schema-level metrics from committed relational state without loading complete models. */
public class SemanticModelMetricService {
  private static final SemanticModelMetricService INSTANCE = new SemanticModelMetricService();

  private SemanticModelMetricService() {}

  /** Returns the metric discovery service. */
  public static SemanticModelMetricService getInstance() {
    return INSTANCE;
  }

  /**
   * Lists live metric identifiers without reading definition snapshots.
   *
   * @param namespace The full schema namespace.
   * @return Metric identifiers with the containing model as the last namespace level.
   * @throws NoSuchEntityException If the schema or an ancestor does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listSemanticModelMetrics")
  public List<NameIdentifier> listMetrics(Namespace namespace) {
    return read(namespace, false).stream()
        .filter(po -> po.getMetricName() != null)
        .map(
            po ->
                NameIdentifier.of(
                    metricNamespace(namespace, po.getSemanticModelName()), po.getMetricName()))
        .toList();
  }

  /**
   * Lists complete metrics from current snapshots, decoding every array before returning.
   *
   * @param namespace The full schema namespace.
   * @return Decoded metrics and their containing namespaces.
   * @throws NoSuchEntityException If the schema or an ancestor does not exist.
   * @throws IllegalStateException If a current snapshot is missing or metrics cannot be decoded.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listSemanticModelMetricInfos")
  public List<SemanticModelMetric> listMetricInfos(Namespace namespace) {
    List<SemanticModelMetric> results = new ArrayList<>();
    for (SemanticModelMetricListingPO po : read(namespace, true)) {
      if (po.getSemanticModelId() == null) {
        continue;
      }
      Preconditions.checkState(
          po.getVersion() != null,
          "Missing current snapshot for Semantic Model %s",
          po.getSemanticModelId());
      if (po.getMetrics() == null) {
        continue;
      }
      Namespace metricNamespace = metricNamespace(namespace, po.getSemanticModelName());
      try {
        MetricDTO[] metrics =
            JsonUtils.anyFieldMapper().readValue(po.getMetrics(), MetricDTO[].class);
        Preconditions.checkArgument(metrics != null, "metrics JSON must be an array");
        for (MetricDTO metric : metrics) {
          Preconditions.checkArgument(metric != null, "metrics must not contain null");
          results.add(new ImmutableSemanticModelMetric(metricNamespace, metric.toMetric()));
        }
      } catch (JsonProcessingException | IllegalArgumentException e) {
        throw new IllegalStateException(
            "Failed to decode metrics for Semantic Model "
                + NameIdentifier.of(metricNamespace.levels()),
            e);
      }
    }
    return results;
  }

  private List<SemanticModelMetricListingPO> read(Namespace namespace, boolean details) {
    NamespaceUtil.checkSemanticModel(namespace);
    String schemaName =
        HierarchicalSchemaUtil.logicalToPhysical(
            namespace.level(2), HierarchicalSchemaUtil.schemaSeparator());
    List<SemanticModelMetricListingPO> rows =
        SessionUtils.getWithoutCommit(
            SemanticModelMetricMapper.class,
            mapper ->
                details
                    ? mapper.listMetricSnapshots(namespace.level(0), namespace.level(1), schemaName)
                    : mapper.listMetricNames(namespace.level(0), namespace.level(1), schemaName));
    if (rows.isEmpty() || rows.get(0) == null || rows.get(0).getSchemaId() == null) {
      throw new NoSuchEntityException("Schema %s does not exist", namespace);
    }
    return rows;
  }

  private Namespace metricNamespace(Namespace schema, String modelName) {
    return Namespace.of(schema.level(0), schema.level(1), schema.level(2), modelName);
  }
}
