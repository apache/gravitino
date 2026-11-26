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
package org.apache.gravitino.rel;

import java.util.Map;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.expressions.transforms.Transform;

/**
 * Iceberg tables that expose every registered partition spec, not only the current default.
 *
 * <p>Iceberg keeps historical specs after partition evolution. Maintenance operations such as
 * rewrite manifests select a {@code spec_id}; callers use this capability to list valid IDs and
 * their transforms.
 */
@Evolving
public interface SupportsIcebergPartitionSpecs {

  /**
   * Returns the current default partition spec ID for the table.
   *
   * @return the default spec ID
   */
  int defaultPartitionSpecId();

  /**
   * Returns every partition spec registered on the table, keyed by Iceberg {@code spec-id}.
   *
   * <p>Each value is the Gravitino transform list for that spec. The map is never null and never
   * empty for a loaded Iceberg table (unpartitioned tables still have spec {@code 0}).
   *
   * @return an unmodifiable map from spec ID to transforms
   */
  Map<Integer, Transform[]> partitionSpecs();
}
