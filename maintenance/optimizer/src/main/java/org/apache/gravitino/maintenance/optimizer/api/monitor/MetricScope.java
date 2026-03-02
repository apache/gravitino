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

package org.apache.gravitino.maintenance.optimizer.api.monitor;

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;

/** Scope of metrics being evaluated (table, partition, or job). */
@DeveloperApi
public final class MetricScope {
  /** Supported scope kinds for monitor evaluation. */
  public enum Type {
    TABLE,
    PARTITION,
    JOB
  }

  private final NameIdentifier identifier;
  private final Type type;
  private final Optional<PartitionPath> partition;

  private MetricScope(NameIdentifier identifier, Type type, Optional<PartitionPath> partition) {
    Preconditions.checkArgument(identifier != null, "identifier must not be null");
    Preconditions.checkArgument(type != null, "type must not be null");
    Preconditions.checkArgument(partition != null, "partition must not be null");
    this.identifier = identifier;
    this.type = type;
    this.partition = partition;
  }

  /**
   * Create a table scope.
   *
   * @param identifier table identifier
   * @return table metric scope
   */
  public static MetricScope forTable(NameIdentifier identifier) {
    return new MetricScope(identifier, Type.TABLE, Optional.empty());
  }

  /**
   * Create a partition scope.
   *
   * @param identifier table identifier
   * @param partition partition path under the table
   * @return partition metric scope
   */
  public static MetricScope forPartition(NameIdentifier identifier, PartitionPath partition) {
    return new MetricScope(identifier, Type.PARTITION, Optional.of(partition));
  }

  /**
   * Create a job scope.
   *
   * @param identifier job identifier
   * @return job metric scope
   */
  public static MetricScope forJob(NameIdentifier identifier) {
    return new MetricScope(identifier, Type.JOB, Optional.empty());
  }

  /**
   * @return evaluated identifier (table or job).
   */
  public NameIdentifier identifier() {
    return identifier;
  }

  /**
   * @return scope type.
   */
  public Type type() {
    return type;
  }

  /**
   * @return partition path when {@link #type()} is {@link Type#PARTITION}; otherwise empty.
   */
  public Optional<PartitionPath> partition() {
    return partition;
  }
}
