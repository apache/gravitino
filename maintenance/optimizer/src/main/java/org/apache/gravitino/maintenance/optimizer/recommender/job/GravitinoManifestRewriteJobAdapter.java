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
package org.apache.gravitino.maintenance.optimizer.recommender.job;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.ManifestRewriteJobContext;

/** Maps a manifest rewrite decision to the built-in Spark job's arguments. */
public final class GravitinoManifestRewriteJobAdapter implements GravitinoJobAdapter {
  @Override
  public Map<String, String> jobConfig(
      JobExecutionContext context, Map<String, String> submitterConfigs, String schemaSeparator) {
    Preconditions.checkArgument(
        context instanceof ManifestRewriteJobContext,
        "jobExecutionContext must be ManifestRewriteJobContext");
    ManifestRewriteJobContext rewrite = (ManifestRewriteJobContext) context;
    String catalogName =
        IdentifierUtils.getCatalogNameFromTableIdentifier(rewrite.nameIdentifier());
    if (submitterConfigs == null) {
      submitterConfigs = Collections.emptyMap();
    }
    Map<String, String> config = new LinkedHashMap<>();
    config.putAll(
        IcebergSparkConfigUtils.resolveTemplateSparkPlaceholders(
            catalogName,
            IcebergSparkConfigUtils.parseFlatJsonMap(
                submitterConfigs.get("spark_conf"), "spark-conf"),
            submitterConfigs));
    config.putAll(rewrite.jobOptions());
    config.put("catalog_name", catalogName);
    config.put(
        "table_identifier",
        IdentifierUtils.toSparkTableIdentifier(rewrite.nameIdentifier(), schemaSeparator));
    return Map.copyOf(config);
  }
}
