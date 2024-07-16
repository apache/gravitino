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
package org.apache.gravitino;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.partitions.Partition;

public class TestTableOperations implements TableOperations, SupportsPartitions {

  private static final Map<String, Partition> partitions = Maps.newHashMap();

  @Override
  public String[] listPartitionNames() {
    return partitions.keySet().toArray(new String[0]);
  }

  @Override
  public Partition[] listPartitions() {
    return partitions.values().toArray(new Partition[0]);
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    if (!partitions.containsKey(partitionName)) {
      throw new NoSuchPartitionException("Partition not found: %s", partitionName);
    }
    return partitions.get(partitionName);
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    if (partitions.containsKey(partition.name())) {
      throw new PartitionAlreadyExistsException("Partition already exists: %s", partition.name());
    }
    partitions.put(partition.name(), partition);
    return partition;
  }

  @Override
  public boolean dropPartition(String partitionName) {
    if (!partitions.containsKey(partitionName)) {
      return false;
    }
    partitions.remove(partitionName);
    return true;
  }

  @Override
  public void close() throws IOException {
    partitions.clear();
  }
}
