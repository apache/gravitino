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

package org.apache.gravitino.maintenance.optimizer.recommender.table;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.recommender.TableMetadataProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoClientUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
import org.apache.gravitino.rel.Table;

/** Table metadata provider backed by Gravitino catalog tables. */
public class GravitinoTableMetadataProvider implements TableMetadataProvider {
  public static final String NAME = "gravitino-table-metadata-provider";
  private GravitinoClient gravitinoClient;

  /**
   * Initializes the provider with a Gravitino client derived from the optimizer configuration.
   *
   * @param optimizerEnv optimizer environment
   */
  @Override
  public void initialize(OptimizerEnv optimizerEnv) {
    this.gravitinoClient = GravitinoClientUtils.createClient(optimizerEnv);
  }

  /**
   * Loads table metadata for the given table identifier.
   *
   * @param tableIdentifier fully qualified table identifier
   * @return table metadata
   */
  @Override
  public Table tableMetadata(NameIdentifier tableIdentifier) {
    IdentifierUtils.requireTableIdentifierNormalized(tableIdentifier);
    return gravitinoClient
        .loadCatalog(IdentifierUtils.getCatalogNameFromTableIdentifier(tableIdentifier))
        .asTableCatalog()
        .loadTable(IdentifierUtils.removeCatalogFromIdentifier(tableIdentifier));
  }

  /**
   * Returns the provider name for configuration lookup.
   *
   * @return provider name
   */
  @Override
  public String name() {
    return NAME;
  }

  /**
   * Closes the underlying Gravitino client.
   *
   * @throws Exception if closing fails
   */
  @Override
  public void close() throws Exception {
    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }
}
