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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.SemanticModelDatasetListing;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.SemanticModelSourceMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelSourceListingPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/** Indexed reverse lookup of current Semantic Model datasets. */
public final class SemanticModelSourceService {
  private static final SemanticModelSourceService INSTANCE = new SemanticModelSourceService();

  private SemanticModelSourceService() {}

  /**
   * @return The singleton source association service.
   */
  public static SemanticModelSourceService getInstance() {
    return INSTANCE;
  }

  /**
   * Lists the live model projections directly associated with an existing registered source.
   *
   * @param source The full source identifier.
   * @param type TABLE or VIEW.
   * @return One projection per matching model.
   * @throws NoSuchEntityException If the source or one of its parents does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listSemanticModelDatasetsBySource")
  public List<SemanticModelDatasetListing> listForSource(
      NameIdentifier source, Entity.EntityType type) {
    Preconditions.checkArgument(
        type == Entity.EntityType.TABLE || type == Entity.EntityType.VIEW,
        "Source must be a table or View");
    long sourceId = EntityIdService.getEntityId(source, type);
    long metalakeId =
        EntityIdService.getEntityId(
            NameIdentifier.of(source.namespace().level(0)), Entity.EntityType.METALAKE);
    List<SemanticModelSourceListingPO> rows =
        SessionUtils.getWithoutCommit(
            SemanticModelSourceMapper.class,
            mapper -> mapper.listForSource(metalakeId, type.name(), sourceId));
    Map<Long, List<SemanticModelSourceListingPO>> groups = new LinkedHashMap<>();
    rows.forEach(
        row -> groups.computeIfAbsent(row.getSemanticModelId(), key -> new ArrayList<>()).add(row));
    List<SemanticModelDatasetListing> result = new ArrayList<>();
    groups.forEach(
        (id, matches) -> {
          SemanticModelSourceListingPO first = matches.get(0);
          NameIdentifier model =
              NameIdentifier.of(
                  first.getMetalakeName(),
                  first.getCatalogName(),
                  HierarchicalSchemaUtil.physicalToLogical(
                      first.getSchemaName(), HierarchicalSchemaUtil.schemaSeparator()),
                  first.getSemanticModelName());
          result.add(
              new SemanticModelDatasetListing(
                  id,
                  model,
                  matches.stream()
                      .map(SemanticModelSourceListingPO::getDatasetName)
                      .distinct()
                      .toList()));
        });
    return result;
  }
}
