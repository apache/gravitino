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
package org.apache.gravitino.catalog;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.rel.SupportsPartitionSpecs;
import org.apache.gravitino.rel.expressions.transforms.Transform;

/**
 * Server-side entry for {@link SupportsPartitionSpecs}, adding {@link NameIdentifier} so the
 * catalog {@link org.apache.gravitino.utils.IsolatedClassLoader} can be resolved — the same pattern
 * as {@link PartitionDispatcher} for {@link org.apache.gravitino.rel.SupportsPartitions}.
 */
public interface PartitionSpecDispatcher {

  /**
   * Returns the current default partition spec ID for the table.
   *
   * @param tableIdent The table identifier.
   * @return the default spec ID
   * @throws UnsupportedOperationException if the table does not expose partition specs
   */
  int defaultPartitionSpecId(NameIdentifier tableIdent);

  /**
   * Returns every partition spec registered on the table, keyed by {@code spec-id}.
   *
   * @param tableIdent The table identifier.
   * @return an unmodifiable map from spec ID to transforms
   * @throws UnsupportedOperationException if the table does not expose partition specs
   */
  Map<Integer, Transform[]> partitionSpecs(NameIdentifier tableIdent);
}
