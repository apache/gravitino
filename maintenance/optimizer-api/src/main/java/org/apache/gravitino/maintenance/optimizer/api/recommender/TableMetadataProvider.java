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

package org.apache.gravitino.maintenance.optimizer.api.recommender;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.maintenance.optimizer.api.common.Provider;
import org.apache.gravitino.rel.Table;

/**
 * Supplies table definitions to {@link StrategyHandler} implementations. The recommender calls this
 * provider only when a handler declares {@code TABLE_METADATA} in {@link
 * StrategyHandler#dataRequirements()}.
 */
@DeveloperApi
public interface TableMetadataProvider extends Provider {

  /**
   * Fetch table metadata for a fully-qualified table identifier.
   *
   * @param tableIdentifier catalog/schema/table identifier (must be three levels)
   * @return table definition
   * @throws NoSuchTableException if the table does not exist
   */
  Table tableMetadata(NameIdentifier tableIdentifier) throws NoSuchTableException;
}
