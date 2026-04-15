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

package org.apache.gravitino.maintenance.optimizer.api.common;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Generic scope for data observations such as metrics and statistics. */
@DeveloperApi
public final class DataScope {
  /** Supported scope kinds. */
  public enum Type {
    TABLE,
    PARTITION,
    JOB
  }

  private final NameIdentifier identifier;
  private final Type type;
  private final Optional<PartitionPath> partition;

  private DataScope(NameIdentifier identifier, Type type, Optional<PartitionPath> partition) {
    Preconditions.checkArgument(identifier != null, "identifier must not be null");
    Preconditions.checkArgument(type != null, "type must not be null");
    Preconditions.checkArgument(partition != null, "partition must not be null");
    Preconditions.checkArgument(
        (type == Type.PARTITION) == partition.isPresent(),
        "partition must be present only for PARTITION scope");
    this.identifier = identifier;
    this.type = type;
    this.partition = partition;
  }

  public static DataScope forTable(NameIdentifier identifier) {
    return new DataScope(identifier, Type.TABLE, Optional.empty());
  }

  public static DataScope forPartition(NameIdentifier identifier, PartitionPath partition) {
    return new DataScope(identifier, Type.PARTITION, Optional.of(partition));
  }

  public static DataScope forJob(NameIdentifier identifier) {
    return new DataScope(identifier, Type.JOB, Optional.empty());
  }

  public NameIdentifier identifier() {
    return identifier;
  }

  public Type type() {
    return type;
  }

  public Optional<PartitionPath> partition() {
    return partition;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof DataScope)) {
      return false;
    }
    DataScope other = (DataScope) obj;
    return Objects.equals(identifier, other.identifier)
        && type == other.type
        && Objects.equals(partition, other.partition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier, type, partition);
  }

  @Override
  public String toString() {
    return "DataScope{"
        + "identifier="
        + identifier
        + ", type="
        + type
        + ", partition="
        + partition
        + '}';
  }
}
